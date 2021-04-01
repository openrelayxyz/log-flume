package notify

import (
  "strings"
  "github.com/Shopify/sarama"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/ethdb/cdc"
  "github.com/ethereum/go-ethereum/event"
)

func SendKafkaNotifications(feed event.Feed, target string) error {
  ch := make(chan common.Hash, 256)
  parts := strings.Split(target, ";")
  brokers, config := cdc.ParseKafkaURL(parts[0])
  if err := cdc.CreateTopicIfDoesNotExist(parts[0], parts[1], 1, make(map[string]*string)); err != nil {
    return err
  }
  producer, err := sarama.NewAsyncProducer(brokers, config)
  if err != nil {
    return err
  }
  feed.Subscribe(ch)
  go func() {
    for hash := range ch {
      producer.Input() <- &sarama.ProducerMessage{Topic: parts[1], Value: sarama.ByteEncoder(hash.Bytes()) }
    }
  }()
  return nil
}
