package main

import (
  "context"
  "github.com/NYTimes/gziphandler"
  "os"
  "./flumehandler"
  "./logfeed"
  "./indexer"
  "net/http"
  "flag"
  "fmt"
  "time"
  "log"
  _ "github.com/mattn/go-sqlite3"
  "database/sql"
  "os/signal"
  "syscall"
  "github.com/rs/cors"
)

func main() {
  // shutdownSync := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
  port := flag.Int("port", 8000, "Serving port")
  flag.CommandLine.Parse(os.Args[1:])
  sqlitePath := flag.CommandLine.Args()[0]
  feedURL := flag.CommandLine.Args()[1]

  logsdb, err := sql.Open("sqlite3", fmt.Sprintf("file:%v?_sync=0", sqlitePath))
  if err != nil { log.Fatalf(err.Error()) }

  var resumeBlock uint64
  err = logsdb.QueryRowContext(context.Background(), "SELECT max(blockNumber) FROM event_logs;").Scan(&resumeBlock)

  feed, err := logfeed.ResolveFeed(feedURL, resumeBlock)
  if err != nil { log.Fatalf(err.Error()) }


  go indexer.ProcessFeed(feed, logsdb)

  handler := flumehandler.GetHandler(logsdb)

  mux := http.NewServeMux()
  mux.HandleFunc("/", handler)
  s := &http.Server{
    Addr: fmt.Sprintf(":%v", *port),
    Handler: gziphandler.GzipHandler(cors.Default().Handler(mux)),
    ReadHeaderTimeout: time.Second,
    MaxHeaderBytes: 1 << 20,
  }
  <-feed.Ready()
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
  go s.ListenAndServe()
  log.Printf("Serving logs on %v", *port)
  <-sigs
  time.Sleep(time.Second)
}
