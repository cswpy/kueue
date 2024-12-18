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
	"sync"

	proto1 "google.golang.org/protobuf/proto"
)

type Persister struct {
	BaseDir            string
	mu                 sync.Mutex
	NumMessagePerBatch int
}

func (p *Persister) loadPersistedData(concurrentMap *ConcurrentMap[string, []*proto.ConsumerMessage]) error {
	// Walk through the directories for each topic partition
	return filepath.WalkDir(p.BaseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			// Check if directory represents a topic partition (e.g., "topic1-0")
			dirName := d.Name()
			if strings.Contains(dirName, "-") && !strings.HasSuffix(dirName, "-offset") {
				// Load messages for this topic partition
				messages, err := p.loadMessagesForPartition(dirName)
				if err != nil {
					return err
				}

				// Update the concurrent map in-place
				concurrentMap.Set(dirName, messages)
			}
		}
		return nil
	})
}

func (p *Persister) loadMessagesForPartition(topicPartitionID string) ([]*proto.ConsumerMessage, error) {
	dirPath := filepath.Join(p.BaseDir, topicPartitionID)

	// Read all files in the partition directory
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Sort files to maintain order
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	var topicPartitionData []*proto.ConsumerMessage

	// Read messages from each file
	for _, file := range files {
		filePath := filepath.Join(dirPath, file.Name())
		messages, err := p.readMessagesFromFile(filePath)
		if err != nil {
			return nil, err
		}
		topicPartitionData = append(topicPartitionData, messages...)
	}

	return topicPartitionData, nil
}

func (p *Persister) readMessagesFromFile(filePath string) ([]*proto.ConsumerMessage, error) {
	file, err := os.Open(filePath)
	if err != nil {
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
			return nil, err
		}

		dataBytes := make([]byte, length)
		_, err = io.ReadFull(file, dataBytes)
		if err != nil {
			return nil, err
		}

		msg := &proto.ConsumerMessage{}
		err = proto1.Unmarshal(dataBytes, msg)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
		messageCount++
	}

	return messages, nil
}

func (p *Persister) persistData(topicPartitionId string, msgs ...*proto.ConsumerMessage) error {
    if len(msgs) == 0 {
        return nil
    }

    p.mu.Lock()
    defer p.mu.Unlock()

    dirPath := filepath.Join(p.BaseDir, topicPartitionId)
    err := os.MkdirAll(dirPath, 0755)
    if err != nil {
        return err
    }

    var currentFile *os.File
    // var currentFileIndex int
    offset := int(msgs[0].Offset)
    baseIndex := (offset / p.NumMessagePerBatch) * p.NumMessagePerBatch
    fileName := fmt.Sprintf("%010d.bin", baseIndex)
    filePath := filepath.Join(dirPath, fileName)
    currentFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer currentFile.Close()

    for _, msg := range msgs {
        // If we've hit a new batch boundary
        if msg.Offset != int32(offset) { // This would be unusual, but check logic if needed
        }

        // If offset hits multiple of batch size, rotate
        if offset != baseIndex && offset%p.NumMessagePerBatch == 0 {
            currentFile.Sync()
            currentFile.Close()

            baseIndex = (offset / p.NumMessagePerBatch) * p.NumMessagePerBatch
            fileName = fmt.Sprintf("%010d.bin", baseIndex)
            filePath = filepath.Join(dirPath, fileName)
            currentFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
            if err != nil {
                return err
            }
        }

        dataBytes, err := proto1.Marshal(msg)
        if err != nil {
            return err
        }

        length := uint32(len(dataBytes))
        if err := binary.Write(currentFile, binary.LittleEndian, length); err != nil {
            return err
        }
        if _, err := currentFile.Write(dataBytes); err != nil {
            return err
        }

        offset++
    }

    if currentFile != nil {
        if err := currentFile.Sync(); err != nil {
            return err
        }
    }

    return nil
}


func (p *Persister) persistConsumerOffset(topicPartitionId, consumerId string, offset int32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Directory for offsets
	dirPath := filepath.Join(p.BaseDir, topicPartitionId+"-offset")
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}

	fileName := fmt.Sprintf("%s.bin", consumerId)
	filePath := filepath.Join(dirPath, fileName)

	// b.logger.Printf("Persisting offset %d for consumer %s in %s", offset, consumerId, filePath)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the offset as binary (int32)
	if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	// b.logger.Printf("Successfully persisted offset %d for consumer %s to file: %s", offset, consumerId, filePath)
	return nil
}

func (p *Persister) loadConsumerOffsets(offsets map[string]int32) error {
	// Check if the base directory exists
	if _, err := os.Stat(p.BaseDir); err != nil {
		return err
	}

	// Walk through directories
	return filepath.WalkDir(p.BaseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Look for directories with "-offset" suffix
		if d.IsDir() && strings.HasSuffix(d.Name(), "-offset") {
			// Extract topic partition ID
			topicPartitionID := strings.TrimSuffix(d.Name(), "-offset")

			// Load offsets for this topic partition
			err := p.loadOffsetsForPartition(topicPartitionID, offsets)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (p *Persister) loadOffsetsForPartition(topicPartitionID string, offsets map[string]int32) error {
	offsetDirPath := filepath.Join(p.BaseDir, topicPartitionID+"-offset")

	// Read all files in the offset directory
	files, err := os.ReadDir(offsetDirPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		filePath := filepath.Join(offsetDirPath, file.Name())
		consumerID := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))

		offset, err := p.readOffsetFromFile(filePath)
		if err != nil {
			return err
		}

		// Construct the key and update the map in place
		consumerOffsetKey := fmt.Sprintf("%s-%s", consumerID, topicPartitionID)
		offsets[consumerOffsetKey] = offset
	}

	return nil
}

func (p *Persister) readOffsetFromFile(filePath string) (int32, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	var offset int32
	err = binary.Read(file, binary.LittleEndian, &offset)
	if err != nil {
		return -1, err
	}
	return offset, nil
}
