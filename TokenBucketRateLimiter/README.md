# Token Bucket Rate Limiter

A lightweight, concurrency-safe Token Bucket rate limiter implemented in Go using only the standard library.

Built as part of [Smithing Ground](https://github.com/nishankx/smithing-ground), a collection of backend engineering and systems programming implementations.

## Overview

The Token Bucket algorithm controls the rate at which operations can be performed.

It maintains a bucket containing a finite number of tokens. Each operation consumes tokens, while tokens are continuously replenished at a configured rate.

When the bucket runs out of tokens, incoming requests are rejected until sufficient tokens become available.

### Example

Consider a bucket configured with:

- Capacity: 10 tokens
- Refill rate: 2 tokens per second

Initially, the bucket contains 10 tokens.

If 10 requests arrive immediately, all 10 are accepted.

Any subsequent request is rejected until a token is replenished.

After 2 seconds, the bucket receives 4 tokens and can accept another 4 requests.

## Features

- Configurable bucket capacity.
- Configurable token replenishment rate.
- Thread-safe token consumption.
- Atomic batch consumption through `AllowN`.
- Fractional token replenishment.
- Injectable clock for deterministic testing.
- Input validation and descriptive errors.
- No external dependencies.

## Installation

```bash
go get github.com/nishankx/smithing-ground/token-bucket
```

## Usage

```go
package main

import (
    "fmt"
    "log"

    tokenbucket "github.com/nishankx/smithing-ground/token-bucket"
)

func main() {
    limiter, err := tokenbucket.NewLimiter(10, 2)
    if err != nil {
        log.Fatal(err)
    }

    if limiter.Allow() {
        fmt.Println("Request allowed")
    } else {
        fmt.Println("Rate limit exceeded")
    }
}
```

## API Reference

| Method | Description |
|---|---|
| `NewLimiter(capacity, rate)` | Creates a new limiter |
| `Allow()` | Attempts to consume one token |
| `AllowN(n)` | Atomically consumes n tokens |
| `Tokens()` | Returns available tokens |
| `Capacity()` | Returns maximum bucket capacity |
| `RefillRate()` | Returns tokens replenished per second |

## Implementation Details

**Algorithm:** Token Bucket

**Concurrency:** `sync.Mutex`

**Time management:** `time.Time`

**Testing:** Injectable clock

**Dependencies:** Go standard library

## Running Tests

Run all tests:

```bash
go test ./...
```

Run with race detection:

```bash
go test -race ./...
```

Check coverage:

```bash
go test -cover ./...
```

Generate a detailed coverage report:

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Project Structure

```text
token-bucket/
├── go.mod
├── limiter.go
├── limiter_test.go
├── example/
│   └── main.go
└── README.md
```

## Design Limitations

This implementation is intentionally in-memory and process-local.

- Token state is not persisted.
- Multiple application instances do not share token state.
- Restarting the application resets the bucket.
- It does not provide distributed rate limiting.

These limitations are deliberate to keep the implementation focused on the underlying algorithm.

## License

MIT
