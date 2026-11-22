# redis

A Redis-compatible server written in Rust, built up incrementally as I work through the Redis protocol.

## Requirements

- Rust 1.96 or newer

## Running

The project has a Tokio-based async implementation.

```sh
cargo run
```

It listen on `127.0.0.1:6379` by default.

For release builds:

```sh
cargo run --release
```

## Trying it out

With `redis-cli` installed:

```sh
redis-cli ping
# PONG
```
