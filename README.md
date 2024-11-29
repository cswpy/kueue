# Kueue: a simple Message Queue implementation

This project is developed for the course CPSC 526 Distributed Systems at Yale University.

## Compiling protobuf and generate gRPC service code

Install all required dependencies by running `go mod tidy`, then run `Makefile` after updating the proto files.

## Structure

```
cmd/
├─ kueue/	CLI to interface with the MQ
docs/		Documentation on the usage
internal/	internally shared code
pkg/
├─ producer/	Producer client API
├─ consumer/	Consumer client API
scripts/
```

## Contributors
Pengyu Wang
Bowen Shi
Yizheng Shi





