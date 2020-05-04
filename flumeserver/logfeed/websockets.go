package logfeed

import (
  "encoding/json"
  "context"
  "database/sql"
  "github.com/gorilla/websocket"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "net/http"
  "sync/atomic"
  "strings"
  "time"
  "log"
  "strconv"
  "fmt"
)

const (
  pingPeriod = 30 * time.Second
)

type ethWSFeed struct {
  urlStr string
  blockFeed event.Feed
  logFeed event.Feed
  connected bool
  resumeBlock uint64
  lastBlockTime *atomic.Value
  ready chan struct{}
}

type rpcResponse struct {
  Version string `json:"jsonrpc"`
  ID *json.RawMessage `json:"id,omitempty"`
  Method string `json:"method,omitempty"`
  Result interface{} `json:"result,omitempty"`
  Params *rpcParams `json:"params,omitempty"`
  Error map[string]interface{} `json:"error,omitempty"`
}

type rpcParams struct {
  Subscription string `json:"subscription,omitempty"`
  Result interface{} `json:"result,omitempty"`
}

func restructureLogs(input interface{}) ([]types.Log, error) {
  result := []types.Log{}
  payload, err := json.Marshal(input)
  if err != nil { return result, err }
  err = json.Unmarshal(payload, &result)
  return result, err
}

func NewETHWSFeed(urlStr string, db *sql.DB) Feed {
  var resumeBlock uint64
  db.QueryRowContext(context.Background(), "SELECT max(blockNumber) FROM event_logs;").Scan(&resumeBlock)
  if resumeBlock > 0 { resumeBlock-- }
  feed := &ethWSFeed{
    urlStr: urlStr,
    lastBlockTime: &atomic.Value{},
    ready: make(chan struct{}),
    resumeBlock: resumeBlock,
  }
  feed.subscribe()
  return feed
}

func (f *ethWSFeed) SubscribeLogs(c chan types.Log) event.Subscription {
  return f.logFeed.Subscribe(c)
}

func (f *ethWSFeed) SubscribeBlocks(c chan map[string]interface{}) event.Subscription {
  return f.blockFeed.Subscribe(c)
}

func (f *ethWSFeed) Ready() chan struct{} {
  return f.ready
}

func (f *ethWSFeed) Healthy(d time.Duration) bool {
  lastBlockTime, ok := f.lastBlockTime.Load().(time.Time)
  if !ok {
    log.Printf("ethWSFeed unhealthy - lastBlockTime is not a time.Time")
    return false
  } else if time.Since(lastBlockTime) > d {
    log.Printf("ethWSFeed unhealthy - No blocks received in timeout")
    return false
  }
  return true
}

// Commit is a noop in websockets - In Kafka this enables offset tracking.
func (f *ethWSFeed) Commit(num uint64) {}

func (f *ethWSFeed) subscribe() {
  dialer := &websocket.Dialer{
    EnableCompression: true,
    Proxy: http.ProxyFromEnvironment,
    HandshakeTimeout: 45 * time.Second,
  }
  time.Sleep(1 * time.Second)
  go func () {
    reconnectTimer := time.Now()
    for {
      f.connected = false
      ctx, cancel := context.WithCancel(context.Background())
      conn, _, err := dialer.Dial(f.urlStr, nil)
      if err != nil {
        log.Printf("Error establishing connection: %v", err.Error())
        if f.ready != nil {
          time.Sleep(1 * time.Second)
        } else {
          if time.Since(reconnectTimer) > 250 * time.Millisecond {
            log.Fatalf("Failed to re-establish connection in 250ms.")
          }
        }
        continue
      }
      resetTimer := time.NewTimer(120 * time.Second)

      go func() {
        ticker := time.NewTicker(pingPeriod)
        for {
          select {
          case <- ticker.C:
            conn.WriteMessage(websocket.PingMessage, []byte{})
          case <- ctx.Done():
            log.Printf("Upstream websocket connection closing.")
            return
          case <- resetTimer.C:
            log.Printf("WARN: No messages from replica in 120 seconds. Re-establishing connection.")
            conn.Close()
            return
          }
        }
      }()
      conn.WriteMessage(websocket.TextMessage, []byte(`{"id": 0, "method": "eth_blockNumber"}`))
      _, blockNumberBody, err := conn.ReadMessage()
      blockNumberMsg := &rpcResponse{}
      err = json.Unmarshal(blockNumberBody, blockNumberMsg)
      if err != nil {
        log.Printf("Error unmarshalling %v: %v", string(blockNumberBody), err.Error())
        continue
      }
      blockNumber, err := strconv.ParseInt(blockNumberMsg.Result.(string), 0, 64)
      if err != nil { log.Fatalf("Error parsing block: %v", err.Error()) }

      blockRange := int64(blockNumber) - int64(f.resumeBlock)

      log.Printf("Resuming from %v to %v", f.resumeBlock, blockNumber)

      log.Printf("Range size: %v", (blockRange / 1000) + 1)
      for i := int64(0); i < (blockRange / 1000) + 1; i++ {
        fromBlock := "0x" + strconv.FormatInt(int64(f.resumeBlock) + (i * 1000), 16)
        toBlockInt := int64(f.resumeBlock) + ((i + 1) * 1000)
        toBlock := "0x" + strconv.FormatInt(toBlockInt, 16)
        if toBlockInt > blockNumber {
          toBlock = "latest"
        }
        log.Printf("Resuming - %v to %v", fromBlock, toBlock)
        err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"id": 0, "method": "eth_getLogs", "params": [{"fromBlock": "%v","toBlock": "%v"}]}`, fromBlock, toBlock)))
        _, logBody, err := conn.ReadMessage()
        if err != nil { log.Fatalf(err.Error()) }
        logMsg := &rpcResponse{}
        err = json.Unmarshal(logBody, logMsg)
        if err != nil {
          log.Printf("Error unmarshalling %v: %v", string(blockNumberBody), err.Error())
          continue
        }
        logs, err := restructureLogs(logMsg.Result)
        if err != nil { log.Fatalf(err.Error() ) }
        for _, logRecord := range logs {
          resetTimer.Reset(120 * time.Second)
          f.logFeed.Send(logRecord)
        }
      }

      f.ready <- struct{}{}
      f.ready = nil

      conn.WriteMessage(websocket.TextMessage, []byte(`{"id": 1, "method": "eth_subscribe", "params": ["newHeads"]}`))
      conn.WriteMessage(websocket.TextMessage, []byte(`{"id": 2, "method": "eth_subscribe", "params": ["logs", {}]}`))
      var (
        headSubID string
        logSubID string
        ok bool
      )
      f.connected = true
      for {
        _, body, err := conn.ReadMessage()
        if err != nil && (websocket.IsCloseError(err, 1006, 1005) || strings.HasSuffix(err.Error(), "use of closed network connection")) {
          log.Printf("Upstream Connection closed")
          reconnectTimer = time.Now()
          f.connected = false
          break
        } else if err != nil {
          log.Printf("Error reading message: %v", err.Error())
          continue
        }

        // Evidently timer.Reset() is nearly impossible to use correctly. I
        // think the worst case though is a race condition where if Reset() is
        // called closed to the timer executing it may still fire, in which case
        // we're okay, we just establish a new connection. Definitely want to
        // move this behaivor to Kafka.
        resetTimer.Reset(120 * time.Second)
        msg := &rpcResponse{}
        err = json.Unmarshal(body, msg)
        if err != nil {
          log.Printf("Error unmarshalling %v: %v", string(body), err.Error())
          continue
        }
        if msg.ID != nil && string(*msg.ID) == "1" {
          headSubID, ok = msg.Result.(string)
          if !ok {
            log.Printf("Unexpected result in message %v", string(body))
          } else {
            log.Printf("headSubID established: %v", headSubID)
          }
          continue
        } else if msg.ID != nil && string(*msg.ID) == "2" {
          logSubID, ok = msg.Result.(string)
          if !ok {
            log.Printf("Unexpected result in message %v", string(body))
          } else {
            log.Printf("logSubID established: %v", logSubID)
          }
          continue
        } else {
          if msg.Params.Subscription == headSubID{
            f.blockFeed.Send(msg.Params.Result)
            f.lastBlockTime.Store(time.Now())
          } else if msg.Params.Subscription == logSubID {
            payload, err := json.Marshal(msg.Params.Result)
            if err != nil {
              log.Printf("Unexpected error marshalling result: %v", err.Error())
              continue
            }
            logMessage := types.Log{}
            if err := json.Unmarshal(payload, &logMessage); err != nil {
              log.Printf("Unexpected error unmarshalling log: %v", err.Error())
              continue
            }
            f.logFeed.Send(logMessage)
          } else {
            log.Printf("Unknown subscription message: %v", string(body))
          }
        }
      }
      cancel()
    }
  }()
}
