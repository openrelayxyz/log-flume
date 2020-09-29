package datafeed


import (
  "strings"
  "fmt"
  "database/sql"
)

func ResolveFeed(url string, db *sql.DB) (DataFeed, error) {
  if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
    return NewETHWSFeed(url, db)
  } else if strings.HasPrefix(url, "kafka://") {
    return NewKafkaDataFeed(url, db)
  } else if url == "null://" {
    return &NullDataFeed{}, nil
  }
  return nil, fmt.Errorf("Unknown feed type")
}
