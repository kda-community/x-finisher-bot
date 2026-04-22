import asyncio
import random
import logging
import argparse
from enum import Enum
from collections import OrderedDict

from aiolimiter import AsyncLimiter

from pypact.chainweb import Chainweb
from pypact.pact_rest_api import SpvRequest, PactCommand
from pypact.kadena_common import b64_decode, b64_encode
from pypact.pact_result import PactResultContinuation, PactResult
from kadena_block_relayer_lib import BlockRelayer

logger = logging.getLogger(__name__)

DELAY_BETWEEN_ATTEMPTS = 300.0
SPV_TIMEOUT = 600.0
TX_POLL_TIMEOUT = 180.0


def gen_nonce():
    if not hasattr(gen_nonce, "instance_id"):
        gen_nonce.instance_id = b64_encode(random.randbytes(8))

    nonce = b64_encode(random.randbytes(8))
    return "x_finisher_bot:{:s}:{:s}".format(gen_nonce.instance_id,nonce )


class MainnetChainWeb(Chainweb):
    POLL_TIME = 15.0
    MAX_REQUEST_PER_MINUTE = 300
    def __init__(self):
        super().__init__("https://api.chainweb-community.org")

class TestnetChainWeb(Chainweb):
    POLL_TIME = 15.0
    MAX_REQUEST_PER_MINUTE = 60
    def __init__(self):
        super().__init__("https://api.testnet.chainweb-community.org")


class BoundedDict(OrderedDict):
    limit = 10000
    def __setitem__(self, k , value):
        if k in self:
            del self[k]
        if len(self) == self.limit:
            self.popitem(last=False)
        super().__setitem__(k, value)


class Status(Enum):
    PROCESSING = 0
    ERROR = 1
    CONFIRMED = 2

class XFinisherBot:

    def __init__(self, network, intial_backward=2880, blocks_rate=10):
        self.intial_backward = intial_backward
        if network == "mainnet01":
            self.client = MainnetChainWeb()
        elif network == "testnet06":
            self.client = TestnetChainWeb()
        else:
            raise ValueError("Unknown network")
        self.relayer = BlockRelayer(network, cut_poll_delay=5.0)
        self.defpact_status = BoundedDict()
        self.block_throttler = AsyncLimiter(blocks_rate, 1.0)


    async def handle_continuation_task(self, cont):
        pact_id = cont.pactId
        src_ch = int(cont.yield_source)
        dst_ch = int(cont.yield_provenance['targetChainId'])

        def __log_debug(x): logger.debug("{:s}: {:s}".format(pact_id, x))
        def __log_info(x): logger.info("{:s}: {:s}".format(pact_id, x))
        def __log_warning(x): logger.warning("{:s}: {:s}".format(pact_id, x))

        # We want to have two confirmations, or two confirmed errors within 5 minutes
        errors = 0
        confirmations = 0

        self.defpact_status[pact_id] = Status.PROCESSING
        __log_info("Processing")
        # First we sleep a little bit => No need to rush
        await asyncio.sleep(random.uniform(10.0, 30.0))


        for attempts in range(10):

            if confirmations == 2:
                __log_info("Confirmed")
                self.defpact_status[pact_id] = Status.CONFIRMED
                break

            if errors == 2:
                __log_info("Confirmed error")
                self.defpact_status[pact_id] = Status.ERROR
                break

            if attempts != 0:
                await asyncio.sleep(DELAY_BETWEEN_ATTEMPTS)

            try:
                async with asyncio.timeout(SPV_TIMEOUT):
                    proof = await self.client.chains[src_ch].spv_until(SpvRequest(pact_id, dst_ch))

                __log_debug("SPV retrieved")

                cmd = PactCommand.create_continuation(cont, proof, self.client.network_id, sender="kadena-xchain-gas", gasLimit=750)
                cmd.nonce = gen_nonce()
                local_result = await self.client.chains[dst_ch].local(cmd)


                if local_result.error and "already completed" in local_result.error.msg:
                    __log_info("Already completed")
                    # The X-chain was already completed
                    confirmations += 1
                    continue
                elif local_result.error:
                    # Can't finish it for some reasons
                    __log_warning("Cannot be completed for reason:"+local_result.error.msg)
                    errors +=1
                    continue

                # It appears that the defpact can be completed because the Local call is succesfull
                __log_info("Submitting transaction")

                await self.client.chains[dst_ch].send(cmd)

                async with asyncio.timeout(TX_POLL_TIMEOUT):
                    results = await self.client.chains[dst_ch].poll_until(cmd.hash)

                res = results[cmd.hash]
                if res.error and "already completed" in res.error.msg:
                    # Someone completed before use
                    __log_info("Already completed")
                    confirmations += 1
                elif res.error:
                    __log_warning("Cannot be completed for reason:"+res.error.msg)
                    errors += 1
                else:
                    # here the transaction is successful
                    __log_info("Transaction success")
                    confirmations += 1

            except Exception as e:
                __log_warning("Error: {!s}".format(repr(e)))
        else:
            # For some reasons here, affter 10 attemps we don't know what's wrong => remove the PactId from the list
            # Maybe take another chance to retry later.
             __log_warning("Error after 10 attemprs")
             del self.defpact_status[pact_id]

    async def blocks_to_results(self, block_payload_stream):
        first = True
        async for b in block_payload_stream:
            if first or b.header.height % 100 == 0:
                logger.info("Block received {:2d} ({:d})".format(b.header.chainId, b.header.height))
                first = False
            else:
                logger.debug("Block received {:2d} ({:d})".format(b.header.chainId, b.header.height))
            for (_, res) in b.payloadWithOutputs.transactions:
                yield PactResult(b64_decode(res))

    @staticmethod
    async def to_continuations(results_streams):
        async for pr in results_streams:
            if pr.continuation and pr.continuation.step < (pr.continuation.stepCount -1) and pr.continuation.yield_provenance and pr.continuation.yield_provenance:
                yield pr.continuation



    async def do_single_chain_backward(self, chain, blocks_count):
        cut = await self.relayer.get_cut()
        _hash = cut.hashes[str(chain)]

        async def _payloads():
            async for x in self.relayer.blocks_range(chain, _hash.height - blocks_count,  _hash.height-1):
                async with self.block_throttler:
                    yield x

        logger.info("Scanning chain {:2d} ===> {:d} -> {:d}".format(int(chain), _hash.height - blocks_count, _hash.height - 1))

        async for c in self.to_continuations(self.blocks_to_results(_payloads())):
            if c.pactId not in self.defpact_status:
                asyncio.create_task(self.handle_continuation_task(c))

        logger.info("Scanning chain {:2d} ended".format(int(chain)))

    def do_all_chains_backward(self, blocks_count):
        for chain in map(str, range(0,20)):
            asyncio.create_task(self.do_single_chain_backward(chain, blocks_count))

    async def handle_live_blocks(self):
        _payloads = self.relayer.payload_stream()
        async for c in self.to_continuations(self.blocks_to_results(_payloads)):
            if c.pactId not in self.defpact_status:
                asyncio.create_task(self.handle_continuation_task(c))

    async def periodic_backard_task(self):
        while True:
            logger.info("Starting periodic backward task")
            self.do_all_chains_backward(240)
            await asyncio.sleep(3600)

    async def run(self, disable_live=False, disable_initial_backward=False):
        async with self.client:
            async with self.relayer.run():
                if not disable_initial_backward:
                    self.do_all_chains_backward(self.intial_backward)

                if not disable_live:
                    asyncio.create_task(self.periodic_backard_task())
                    await self.handle_live_blocks()
                else:
                    await asyncio.Event().wait()

def parse_args():
    VALID_LEVELS = ["ERROR", "WARNING", "INFO", "DEBUG"]
    parser = argparse.ArgumentParser(prog="x-finisher-bot", description="X Finisher Bot")

    parser.add_argument("--debug", choices=VALID_LEVELS, default="INFO", help="Global debug level (default: INFO)")
    parser.add_argument("--debug-relayer", choices=VALID_LEVELS, default="WARNING", help="Relayer debug level (default: WARNING)")
    parser.add_argument("--network", type=str, default="mainnet01", help="Network name (default: mainnet01)")
    parser.add_argument("--initial-backward-depth", type=int, default=20160, help="Initial backward depth (default: 20160)")
    parser.add_argument("--blocks-per-second", type=int, default=10, help="Maximum block rate when doing backward scanning")
    parser.add_argument("--disable-live", action="store_true", help="Disable live processing (default: False)")
    parser.add_argument("--disable-initial-backward", action="store_true", help="Disable initial backward processing (default: False)")

    return parser.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logger.setLevel(args.debug)
    logging.getLogger("kadena_block_relayer_lib").setLevel(args.debug_relayer)

    bot = XFinisherBot(args.network, intial_backward=args.initial_backward_depth, blocks_rate=args.blocks_per_second)
    asyncio.run(bot.run(disable_live=args.disable_live, disable_initial_backward=args.disable_initial_backward))


if __name__ == "__main__":
    main()
