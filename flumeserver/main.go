package main

import (
  "context"
  "github.com/NYTimes/gziphandler"
  "os"
  "github.com/openrelayxyz/flume/flumeserver/flumehandler"
  "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "github.com/openrelayxyz/flume/flumeserver/indexer"
  gethLog "github.com/ethereum/go-ethereum/log"
  "net/http"
  "flag"
  "fmt"
  "time"
  "log"
  _ "github.com/mattn/go-sqlite3"
  _ "net/http/pprof"
  "database/sql"
  "os/signal"
  "syscall"
  "github.com/rs/cors"
)

func main() {


  glogger := gethLog.NewGlogHandler(gethLog.StreamHandler(os.Stderr, gethLog.TerminalFormat(false)))
	glogger.Verbosity(gethLog.Lvl(3))
	glogger.Vmodule("")
	gethLog.Root().SetHandler(glogger)

  // shutdownSync := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
  port := flag.Int("port", 8000, "Serving port")
  minSafeBlock := flag.Int("min-safe-block", 1000000, "Do not start serving if the smallest block exceeds this value")
  shutdownSync := flag.Bool("shutdown.sync", false, "Sync after shutdown")
  flag.CommandLine.Parse(os.Args[1:])
  sqlitePath := flag.CommandLine.Args()[0]
  feedURL := flag.CommandLine.Args()[1]

  logsdb, err := sql.Open("sqlite3", fmt.Sprintf("file:%v?_sync=0&journal_mode=WAL", sqlitePath))
  if err != nil { log.Fatalf(err.Error()) }
  logsdb.SetConnMaxLifetime(0)
  logsdb.SetMaxIdleConns(32)

  var tableName string
  logsdb.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='event_logs';").Scan(&tableName)
  if tableName != "event_logs" {
    logsdb.Exec("CREATE TABLE event_logs (address varchar(20), topic0 varchar(32), topic1 varchar(32), topic2 varchar(32), topic3 varchar(32), topic4 varchar(32), data blob, blockNumber BIGINT, transactionHash varchar(32), transactionIndex MEDIUMINT, blockHash varchar(32), logIndex MEDIUMINT, PRIMARY KEY (blockHash, logIndex));")
    logsdb.Exec("CREATE INDEX address ON event_logs(address);")
    logsdb.Exec("CREATE INDEX topic0 ON event_logs(topic0);")
    logsdb.Exec("CREATE INDEX topic1 ON event_logs(topic1);")
    logsdb.Exec("CREATE INDEX topic2 ON event_logs(topic2);")
    logsdb.Exec("CREATE INDEX topic3 ON event_logs(topic3);")
    logsdb.Exec("CREATE INDEX topic4 ON event_logs(topic4);")
    logsdb.Exec("CREATE INDEX blockNumber ON event_logs(blockNumber);")
  }

  feed, err := logfeed.ResolveFeed(feedURL, logsdb)
  if err != nil { log.Fatalf(err.Error()) }

  quit := make(chan struct{})
  go indexer.ProcessFeed(feed, logsdb, quit)

  handler := flumehandler.GetHandler(logsdb)

  mux := http.NewServeMux()
  mux.HandleFunc("/", handler)
  s := &http.Server{
    Addr: fmt.Sprintf(":%v", *port),
    Handler: gziphandler.GzipHandler(cors.Default().Handler(mux)),
    ReadHeaderTimeout: 5 * time.Second,
    MaxHeaderBytes: 1 << 20,
  }
  p := &http.Server{
    Addr: ":6969",
    Handler: http.DefaultServeMux,
    ReadHeaderTimeout: 5 * time.Second,
    MaxHeaderBytes: 1 << 20,
  }
  go p.ListenAndServe()
  <-feed.Ready()
  var minBlock int
  logsdb.QueryRowContext(context.Background(), "SELECT min(blockNumber) FROM event_logs;").Scan(&minBlock)
  if minBlock > *minSafeBlock {
    log.Fatalf("Earliest log found on block %v. Should be less than or equal to %v", minBlock, *minSafeBlock)
  }
  if !*shutdownSync {
    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    go s.ListenAndServe()
    log.Printf("Serving logs on %v", *port)
    <-sigs
    time.Sleep(time.Second)
  }
  quit <- struct{}{}
  logsdb.Close()
  time.Sleep(time.Second)
}
