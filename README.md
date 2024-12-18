# Kueue: a simple Message Queue implementation

This project is developed for the course CPSC 526 Distributed Systems at Yale University.

## Run
To run the controller,
```
go run  cmd/controller/controller.go --controller-address="${CONTROLLER_ADDRESS}"
```
`--controller-address` should take the format of `<IP_ADDR>:<PORT>`, which is the address the controller runs on.

To run the broker,
```
go run cmd/broker/broker.go --persist-batch="${PERSIST_BATCH}" --broker-name="${broker_name}" --broker-address="${broker_address}" --controller-address="${CONTROLLER_ADDRESS}"
```
`--persist-batch` is the number of messages to persist in storage a single binary file. `--broker-name` should be a unique identifier of this broker in a cluster. `--broker-address` is where the broker process will run, and will be advertised to peers in the cluster. `--controller-address` is the address to connect to talk to the controller, which should match the address used to run the controller.

To run all broker and controller and test heartbeat,
```
./scripts/test_heartbeat.sh
```
This will run all of them at the same time and peridically test heartbeat from each brokers

The producer and consumer client APIs are located in the `pkg` folder. You can run integration tests for the producer and consumer APIs using the following commands:
```
# Run producer-only integration tests
go test ./pkg/producer -v 

# Run both producer and consumer integration tests
go test ./pkg/consumer -v
```
Make sure all dependencies are installed and the required services (controller and broker) are running before executing the tests.

## Compiling protobuf

Install all required dependencies by running `go mod tidy`, then run `Makefile` after updating the proto files.

## Structure

```
├── LICENSE
├── Makefile            // run `make` to compile proto files
├── README.md
├── cmd                 // cli to run broker and controller 
│   ├── broker
│   │   └── broker.go
│   └── controller
│       └── controller.go
├── docs
│   ├── README.md
│   └── architecture.drawio.svg
├── go.mod
├── go.sum
├── kueue
│   ├── broker.go           // kueue broker
│   ├── broker_test.go
│   ├── clientpool.go
│   ├── concurrent_map.go   // concurrent map used to store messages in memory
│   ├── concurrent_map_test.go
│   ├── controller.go       // kueue controller
│   ├── controller_test.go
│   ├── kueued.go           // data structure definition in kueue
│   ├── logging.go
│   ├── metadata.go
│   ├── persist.go          // persisting messages & offsets to storage
│   ├── proto
│   │   ├── broker.pb.go
│   │   ├── broker.proto
│   │   ├── broker_grpc.pb.go
│   │   ├── controller.pb.go
│   │   ├── controller.proto
│   │   └── controller_grpc.pb.go
│   └── utils.go
├── kueue_writeup.pdf
├── lit_review_evan.pdf
├── lit_review_pengyu.pdf
├── lit_review_yizheng.pdf
├── pkg                     // consumer/producer client API
│   ├── consumer
│   │   ├── consumer.go
│   │   └── consumer_test.go
│   └── producer
│       ├── producer.go
│       └── producer_test.go
├── run.log
└── scripts                 // scripts to test with a cluster
    └── test_heartbeat.sh
```

## Group Work
Pengyu Wang: overall architecture design, broker & controller implementation, protobuffer spec
Bowen Shi: overall architecture design, producer/consumer client API implementation, documentation, write-up
Yizheng Shi: overall architecture design, serialization/deserialization, unit tests, integration tests, cli





