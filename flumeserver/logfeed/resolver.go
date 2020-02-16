package logfeed


import (
  "strings"
  "fmt"
)

func ResolveFeed(url string, resumeBlock uint64) (Feed, error) {
  if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
    return NewETHWSFeed(url, resumeBlock), nil
  }
  return nil, fmt.Errorf("Unknown feed type")
}
