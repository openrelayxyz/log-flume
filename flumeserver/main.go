package main

import (
  "context"
  "github.com/NYTimes/gziphandler"
  "os"
  "github.com/openrelayxyz/flume/flumeserver/flumehandler"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "github.com/openrelayxyz/flume/flumeserver/indexer"
  "github.com/openrelayxyz/flume/flumeserver/migrations"
  "github.com/openrelayxyz/flume/flumeserver/notify"
  gethLog "github.com/ethereum/go-ethereum/log"
  "github.com/ethereum/go-ethereum/event"
  "net/http"
  "path/filepath"
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
  "sync"
)

func main() {

  // shutdownSync := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
  port := flag.Int("port", 8000, "Serving port")
  minSafeBlock := flag.Int("min-safe-block", 1000000, "Do not start serving if the smallest block exceeds this value")
  shutdownSync := flag.Bool("shutdown.sync", false, "Sync after shutdown")
  mainnet := flag.Bool("mainnet", false, "Ethereum Mainnet")
  classic := flag.Bool("classic", false, "Ethereum Classic")
  goerli := flag.Bool("goerli", false, "Goerli Testnet")
  ropsten := flag.Bool("ropsten", false, "Ropsten Testnet")
  rinkeby := flag.Bool("rinkeby", false, "Rinkeby Testnet")
  homesteadBlockFlag := flag.Int("homestead", 0, "Block of the homestead hardfork")
  eip155BlockFlag := flag.Int("eip155", 0, "Block of the eip155 hardfork")
  verbosity := flag.Bool("verbose", false, "Increase verbosity")
  mmap := flag.Int("mmap-size", 1073741824, "Set mmap size")
  cacheSize := flag.Int("cache-size", 2000, "Set cache size (in 4 kb pages")
  memstore := flag.Bool("memstore", false, "Store temporary tables in memory")
  completionTopic := flag.String("completion-topic", "", "A kafka topic to broadcast newly indexed blocks")

  flag.CommandLine.Parse(os.Args[1:])

  glogger := gethLog.NewGlogHandler(gethLog.StreamHandler(os.Stderr, gethLog.TerminalFormat(false)))
  if *verbosity {
    glogger.Verbosity(gethLog.LvlDebug)
  } else {
    glogger.Verbosity(gethLog.LvlInfo)
  }
  glogger.Vmodule("")
  gethLog.Root().SetHandler(glogger)

  var homesteadBlock, eip155Block, chainid uint64

  if *mainnet {
    homesteadBlock = 1150000
    eip155Block = 2675000
    chainid = 1
  } else if *classic {
    homesteadBlock = 1150000
    eip155Block = 3000000
    chainid = 61
  } else if *ropsten {
    homesteadBlock = 0
    eip155Block = 10
    chainid = 3
  } else if *rinkeby {
    homesteadBlock = 1
    eip155Block = 3
    chainid = 4
  } else if *goerli {
    homesteadBlock = 0
    eip155Block = 0
    chainid = 5
  } else {
    homesteadBlock = uint64(*homesteadBlockFlag)
    eip155Block = uint64(*eip155BlockFlag)
    chainid = 0
  }


  sqlitePath := flag.CommandLine.Args()[0]
  feedURL := flag.CommandLine.Args()[1]

  logsdb, err := sql.Open("sqlite3", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=on", sqlitePath))
  if err != nil { log.Fatalf(err.Error()) }
  logsdb.Exec(fmt.Sprintf("pragma mmap_size=%v", *mmap))
  logsdb.Exec(fmt.Sprintf("pragma cache_size=%v", *cacheSize))
  logsdb.Exec(fmt.Sprintf("pragma temp_store_directory = '%v'", filepath.Dir(sqlitePath)))
  if *memstore {
    logsdb.Exec("pragma temp_store = memory")
  }
  logsdb.SetConnMaxLifetime(0)
  logsdb.SetMaxIdleConns(32)
  go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
      stats := logsdb.Stats()
      log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
    }
  }()
  if err := migrations.Migrate(logsdb, chainid); err != nil {
    log.Fatalf(err.Error())
  }
  var completionFeed event.Feed
  feed, err := datafeed.ResolveFeed(feedURL, logsdb)
  if err != nil { log.Fatalf(err.Error()) }

  quit := make(chan struct{})
  // go indexer.ProcessFeed(feed, logsdb, quit)
  wg := &sync.WaitGroup{}
  go indexer.ProcessDataFeed(feed, completionFeed, logsdb, quit, eip155Block, homesteadBlock, wg)


  mux := http.NewServeMux()
  mux.HandleFunc("/", flumehandler.GetHandler(logsdb, wg))
  mux.HandleFunc("/api", flumehandler.GetAPIHandler(logsdb, chainid, wg))
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
  if *completionTopic != "" {
    notify.SendKafkaNotifications(completionFeed, *completionTopic)
  }
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
