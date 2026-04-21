# X-Chain Finisher Bot

This bot auto-detects unfinished X-chain DefPact transactions on Kadena, and finishes them as required.
The bot is 100% autonomous and gathers its data from the Kadena P2P network.

The bot targets maximum reliability, with the objective of zero forgotten pending transactions.

## Run

```
uv run x-finisher-bot
```

or directly from github

```
uv run --with git+https://github.com/kda-community/x-finisher-bot x-finisher-bot
```

```
usage: x-finisher-bot [-h] [--debug {ERROR,WARNING,INFO,DEBUG}] [--debug-relayer {ERROR,WARNING,INFO,DEBUG}] [--network NETWORK] [--initial-backward-depth INITIAL_BACKWARD_DEPTH]
                      [--blocks-per-second BLOCKS_PER_SECOND] [--disable-live] [--disable-initial-backward]

X Finisher Bot

options:
  -h, --help            show this help message and exit
  --debug {ERROR,WARNING,INFO,DEBUG}
                        Global debug level (default: INFO)
  --debug-relayer {ERROR,WARNING,INFO,DEBUG}
                        Relayer debug level (default: WARNING)
  --network NETWORK     Network name (default: mainnet01)
  --initial-backward-depth INITIAL_BACKWARD_DEPTH
                        Initial backward depth (default: 20160)
  --blocks-per-second BLOCKS_PER_SECOND
                        Maximum block rate when doing backward scanning
  --disable-live        Disable live processing (default: False)
  --disable-initial-backward
                        Disable initial backward processing (default: False)
```



## Finish criteria

An X-chain transaction is considered completed when either:
- a submitted continuation transaction was successful
- a local test for a continuation transaction returns "already completed"

To be definitively considered as finished (despite orphaned blocks) by the bot, the transaction must
have been flagged 2 times as "completed" with a 5-minute interval.

## Block streams

Three mechanisms are working in parallel:
- Real-time analysis (transactions to finish are detected as soon as they are mined)
- Periodic backward scan. Every hour, the last 240 blocks are scanned, to find missed pending transactions.
- Initial backward scan. When the bot is started, it scans the last 7 days (can be configured), to find missed pending transactions.