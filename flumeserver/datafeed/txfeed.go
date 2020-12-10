package datafeed

import (
  "github.com/Shopify/sarama"
  "fmt"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/ethdb/cdc"
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/log"
  "time"
)


type KafkaTransactionConsumer struct {
  txs chan TimestampedTx
  consumer sarama.Consumer
  topic string
}

type TimestampedTx struct {
  Tx *types.Transaction
  Timestamp time.Time
}

func (consumer *KafkaTransactionConsumer) Messages() <-chan TimestampedTx {
  if consumer.txs == nil {
    partitions, err := consumer.consumer.Partitions(consumer.topic)
    if err != nil {
      log.Error("Failed to list partitions - Cannot consume transactions", "topic", consumer.topic, "error", err)
      return nil
    }
    consumer.txs = make(chan TimestampedTx, 100)
    for _, partition := range partitions {
      partitionConsumer, err := consumer.consumer.ConsumePartition(consumer.topic, partition, sarama.OffsetOldest)
      if err != nil {
        log.Error("Failed to consume partition", "topic", consumer.topic, "partition", partition, "error", err)
        consumer.txs = nil
        return nil
      }
      go func() {
        for msg := range partitionConsumer.Messages() {
          transaction := &types.Transaction{}
          if err := rlp.DecodeBytes(msg.Value, transaction); err != nil {
            fmt.Printf("Error decoding: %v\n", err.Error())
          }
          consumer.txs <- TimestampedTx{transaction, msg.Timestamp}
        }
      }()
    }
  }
  return consumer.txs
}

func (consumer *KafkaTransactionConsumer) Close() {
  consumer.consumer.Close()
}

func strPtr(x string) *string { return &x }

func NewKafkaTransactionConsumerFromURLs(brokerURL, topic string) (TransactionFeed, error) {
  configEntries := make(map[string]*string)
  configEntries["retention.ms"] = strPtr("3600000")
  configEntries["compression.type"] = strPtr("snappy")
  brokers, config := cdc.ParseKafkaURL(brokerURL)
  client, err := sarama.NewClient(brokers, config)
  if err != nil {
    return nil, err
  }
  consumer, err := sarama.NewConsumerFromClient(client)
  if err != nil {
    return nil, err
  }
  return &KafkaTransactionConsumer{consumer: consumer, topic: topic}, nil
}
