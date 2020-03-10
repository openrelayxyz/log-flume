package logfeed


import (
  "strings"
  "fmt"
  "database/sql"
)

func ResolveFeed(url string, db *sql.DB) (Feed, error) {
  if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
    return NewETHWSFeed(url, db), nil
  } else if strings.HasPrefix(url, "kafka://") {
    return NewKafkaFeed(url, db)
  }
  return nil, fmt.Errorf("Unknown feed type")
}
