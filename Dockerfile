FROM ghcr.io/astral-sh/uv:python3.13-alpine

RUN apk add --no-cache git

CMD ["uv", "run", "--with", "git+https://github.com/kda-community/x-finisher-bot", "x-finisher-bot"]
