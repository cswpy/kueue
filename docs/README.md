# README

## Write-Path

```mermaid
sequenceDiagram
    participant P as Producer
    participant C as Controller
    P->>C: Create a new topic
    C->>C: Assign partitions to broker
    C->>P: Return all partition broker info
    create participant B1 as Broker 1
    P->>B1: Select a broker to produce message
    B1->>B1: Persist
    B1->>P: ACK
    create participant B2 as Broker 2
    B1->>B2: Replicate message
    destroy B2
```

## Read-Path

```mermaid
sequenceDiagram
    participant CTRL as Controller
    participant C as Consumer
    C->>CTRL: Subscribe to a new topic
    CTRL->>CTRL: Assign a partition to the consumer
    CTRL->>C: Return broker info and partition ID
    create participant B1 as Broker 1
    C->>B1: Consume messages from partition ID
    B1->>B1: Persists consumer offset
    B1->>C: Return messages
    C->>B1: ACK/Commit?
```
