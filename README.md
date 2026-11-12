# redis

A Redis-compatible server written in Rust, built up incrementally as I work through the Redis protocol.

## Requirements

- Rust 1.96 or newer

## Running

The project has two implementations of the same server: a thread-per-connection version and a Tokio-based async version.

```sh
cargo run --bin sync
cargo run --bin async
```

Both listen on `127.0.0.1:6379` by default, so run one at a time.

For release builds:

```sh
cargo run --release --bin sync
cargo run --release --bin async
```

## Trying it out

With `redis-cli` installed:

```sh
redis-cli ping
# PONG
```
