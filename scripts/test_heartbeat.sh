#!/bin/bash

set -e

# Set the controller details
CONTROLLER_ADDRESS="127.0.0.1:8080"

# Start the controller
go run -race cmd/controller/controller.go --controller-address="${CONTROLLER_ADDRESS}" &

sleep 1

# Start the brokers with unique names and ports
BROKER_DETAILS=(
    "BR1 127.0.0.1:8081"
    "BR2 127.0.0.1:8082"
    "BR3 127.0.0.1:8083"
)

for detail in "${BROKER_DETAILS[@]}"; do
    IFS=' ' read -r broker_name broker_address <<<"$detail"
    go run -race cmd/broker/broker.go --broker-name="${broker_name}" --broker-address="${broker_address}" --controller-address="${CONTROLLER_ADDRESS}" &
done

# Wait for all background processes to finish
wait

echo "All processes have exited."
