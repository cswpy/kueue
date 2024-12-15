package kueue

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"kueue/kueue/proto"
	"os"
	"path/filepath"
	"sort"
	"strings"

	proto1 "google.golang.org/protobuf/proto"
)

// persist helper functions




func (b *Broker) loadPersistedData() error {
    // Base directory where broker data is stored
    baseDir := b.BrokerInfo.BrokerName

    // Walk through the directories for each topic partition
    return filepath.WalkDir(baseDir, func(path string, d fs.DirEntry, err error) error {
        if err != nil {
            b.logger.Printf("Failed to access path %s: %v", path, err)
            return err
        }

        if d.IsDir() {
            // Check if directory represents a topic partition (e.g., "topic1-0")
            dirName := d.Name()
            if strings.Contains(dirName, "-") && !strings.HasSuffix(dirName, "-offset") {
                // Load messages for this topic partition
                return b.loadMessagesForPartition(dirName)
            }
        }
        return nil
    })
}

func (b *Broker) loadMessagesForPartition(topicPartitionID string) error {
    dirPath := filepath.Join(b.BrokerInfo.BrokerName, topicPartitionID)

    // Get the shard for the partition
    mapShard := b.data.ShardForKey(topicPartitionID)
    mapShard.Lock()
    defer mapShard.Unlock()

    // Initialize or get existing message slice
    var topicPartition []*proto.ConsumerMessage
    if existingData, ok := mapShard.Items[topicPartitionID]; ok {
        topicPartition = existingData
    } else {
        topicPartition = make([]*proto.ConsumerMessage, 0)
    }

    // Read all files in the partition directory
    files, err := os.ReadDir(dirPath)
    if err != nil {
        b.logger.Printf("Failed to read directory %s: %v", dirPath, err)
        return err
    }

    // Sort files to maintain order
    sort.Slice(files, func(i, j int) bool {
        return files[i].Name() < files[j].Name()
    })

    // Read messages from each file
    for _, file := range files {
        filePath := filepath.Join(dirPath, file.Name())
        messages, err := b.readMessagesFromFile(filePath)
        if err != nil {
            b.logger.Printf("Failed to read messages from file %s: %v", filePath, err)
            return err
        }
        topicPartition = append(topicPartition, messages...)
    }

    // Store the loaded messages back to the map
    mapShard.Items[topicPartitionID] = topicPartition
    return nil
}

func (b *Broker) readMessagesFromFile(filePath string) ([]*proto.ConsumerMessage, error) {
    file, err := os.Open(filePath)
    if err != nil {
        b.logger.Printf("Failed to open file %s: %v", filePath, err)
        return nil, err
    }
    defer file.Close()

    var messages []*proto.ConsumerMessage
    messageCount := 0
    for {
        var length uint32
        err := binary.Read(file, binary.LittleEndian, &length)
        if err != nil {
            if err == io.EOF {
                // End of file reached
                break
            }
            b.logger.Printf("Failed to read message length from file %s: %v", filePath, err)
            return nil, err
        }

        dataBytes := make([]byte, length)
        _, err = io.ReadFull(file, dataBytes)
        if err != nil {
            b.logger.Printf("Failed to read message data from file %s: %v", filePath, err)
            return nil, err
        }

        msg := &proto.ConsumerMessage{}
        err = proto1.Unmarshal(dataBytes, msg)
        if err != nil {
            b.logger.Printf("Failed to unmarshal message from file %s: %v", filePath, err)
            return nil, err
        }

        messages = append(messages, msg)
        messageCount++
    }

    // Log the number of messages read from this file
    b.logger.Printf("Read %d messages from file %s", messageCount, filePath)

    return messages, nil
}

func (b *Broker) persistData(topicPartitionId string, msg *proto.ConsumerMessage) {
    b.messageCountMu.Lock()
    defer b.messageCountMu.Unlock()

    dirPath := filepath.Join(b.BrokerInfo.BrokerName, topicPartitionId)
    err := os.MkdirAll(dirPath, 0755)
    if err != nil {
        b.logger.Fatalf("Failed to create directory: %v", err)
        return
    }

    // Serialize the message
    dataBytes, err := proto1.Marshal(msg)
    if err != nil {
        b.logger.Printf("Failed to marshal message: %v", err)
        return
    }

    messageCount := msg.Offset
    fileIndex := (int(messageCount) / b.BrokerInfo.PersistBatch) * b.BrokerInfo.PersistBatch
    fileName := fmt.Sprintf("%010d.bin", fileIndex)
    filePath := filepath.Join(dirPath, fileName)

    // b.logger.Printf("Writing to file: %s, Offset: %d, Length: %d", filePath, msg.Offset, len(dataBytes))

    file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        b.logger.Printf("Failed to open file %s: %v", filePath, err)
        return
    }
    defer file.Close()

    length := uint32(len(dataBytes))
    if err := binary.Write(file, binary.LittleEndian, length); err != nil {
        b.logger.Printf("Failed to write message length to file %s: %v", filePath, err)
        return
    }
    if _, err := file.Write(dataBytes); err != nil {
        b.logger.Printf("Failed to write message to file %s: %v", filePath, err)
        return
    }

    if err := file.Sync(); err != nil {
        b.logger.Printf("Failed to sync file %s: %v", filePath, err)
        return
    }

    // b.logger.Printf("Successfully persisted message with Offset: %d to file: %s", msg.Offset, filePath)
}




func (b *Broker) persistConsumerOffset(topicPartitionId, consumerId string, offset int32) {
    b.messageCountMu.Lock()
    defer b.messageCountMu.Unlock()

    // Directory for offsets
    dirPath := filepath.Join(b.BrokerInfo.BrokerName, topicPartitionId+"-offset")
    err := os.MkdirAll(dirPath, 0755)
    if err != nil {
        b.logger.Fatalf("Failed to create directory: %v", err)
        return
    }

    fileName := fmt.Sprintf("%s.bin", consumerId)
    filePath := filepath.Join(dirPath, fileName)

    // b.logger.Printf("Persisting offset %d for consumer %s in %s", offset, consumerId, filePath)

    file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        b.logger.Printf("Failed to open file %s for offset persistence: %v", filePath, err)
        return
    }
    defer file.Close()

    // Write the offset as binary (int32)
    if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
        b.logger.Printf("Failed to write offset to file %s: %v", filePath, err)
        return
    }

    if err := file.Sync(); err != nil {
        b.logger.Printf("Failed to sync offset file %s: %v", filePath, err)
        return
    }

    // b.logger.Printf("Successfully persisted offset %d for consumer %s to file: %s", offset, consumerId, filePath)
}

func (b *Broker) loadConsumerOffsets() error {
    baseDir := b.BrokerInfo.BrokerName

    // Check if base directory exists
    if _, err := os.Stat(baseDir); os.IsNotExist(err) {
        // No offsets to load; this is expected on first startup
        b.logger.Printf("Data directory %s does not exist. No consumer offsets to load.", baseDir)
        return nil
    } else if err != nil {
        // An unexpected error occurred
        b.logger.Printf("Failed to access data directory %s: %v", baseDir, err)
        return err
    }

    // Proceed to walk through the directories
    return filepath.WalkDir(baseDir, func(path string, d fs.DirEntry, err error) error {
        if err != nil {
            b.logger.Printf("Failed to access path %s: %v", path, err)
            return err
        }

        if d.IsDir() {
            dirName := d.Name()
            if strings.HasSuffix(dirName, "-offset") {
                // Load offsets for this topic partition
                topicPartitionID := strings.TrimSuffix(dirName, "-offset")
                return b.loadOffsetsForPartition(topicPartitionID)
            }
        }
        return nil
    })
}

func (b *Broker) loadOffsetsForPartition(topicPartitionID string) error {
    offsetDirPath := filepath.Join(b.BrokerInfo.BrokerName, topicPartitionID+"-offset")

    files, err := os.ReadDir(offsetDirPath)
    if err != nil {
        b.logger.Printf("Failed to read offset directory %s: %v", offsetDirPath, err)
        return err
    }

    b.offsetLock.Lock()
    defer b.offsetLock.Unlock()

    for _, file := range files {
        filePath := filepath.Join(offsetDirPath, file.Name())
        consumerID := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))

        offset, err := b.readOffsetFromFile(filePath)
        if err != nil {
            b.logger.Printf("Failed to read offset from file %s: %v", filePath, err)
            return err
        }

        // Construct the key for consumer offset map
        consumerOffsetKey := fmt.Sprintf("%s-%s", consumerID, topicPartitionID)
        b.consumerOffset[consumerOffsetKey] = offset

        // Log the loaded offset
        b.logger.Printf("Loaded offset for consumer %s on partition %s: %d", consumerID, topicPartitionID, offset)
    }
    return nil
}


func (b *Broker) readOffsetFromFile(filePath string) (int32, error) {
    file, err := os.Open(filePath)
    if err != nil {
        b.logger.Printf("Failed to open offset file %s: %v", filePath, err)
        return 0, err
    }
    defer file.Close()

    var offset int32
    err = binary.Read(file, binary.LittleEndian, &offset)
    if err != nil {
        b.logger.Printf("Failed to read offset from file %s: %v", filePath, err)
        return 0, err
    }
    return offset, nil
}