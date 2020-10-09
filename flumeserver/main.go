package main

import (
  "context"
  "github.com/NYTimes/gziphandler"
  "os"
  "github.com/openrelayxyz/flume/flumeserver/flumehandler"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "github.com/openrelayxyz/flume/flumeserver/indexer"
  "github.com/openrelayxyz/flume/flumeserver/migrations"
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
  mainnet := flag.Bool("mainnet", false, "Ethereum Mainnet")
  classic := flag.Bool("classic", false, "Ethereum Classic")
  goerli := flag.Bool("goerli", false, "Goerli Testnet")
  ropsten := flag.Bool("ropsten", false, "Ropsten Testnet")
  rinkeby := flag.Bool("rinkeby", false, "Rinkeby Testnet")
  homesteadBlockFlag := flag.Int("homestead", 0, "Block of the homestead hardfork")
  eip155BlockFlag := flag.Int("eip155", 0, "Block of the eip155 hardfork")

  var homesteadBlock, eip155Block uint64

  if *mainnet {
    homesteadBlock = 1150000
    eip155Block = 2675000
  } else if *classic {
    homesteadBlock = 1150000
    eip155Block = 3000000
  } else if *ropsten {
    homesteadBlock = 0
    eip155Block = 10
  } else if *rinkeby {
    homesteadBlock = 1
    eip155Block = 3
  } else if *goerli {
    homesteadBlock = 0
    eip155Block = 0
  } else {
    homesteadBlock = uint64(*homesteadBlockFlag)
    eip155Block = uint64(*eip155BlockFlag)
  }


  flag.CommandLine.Parse(os.Args[1:])
  sqlitePath := flag.CommandLine.Args()[0]
  feedURL := flag.CommandLine.Args()[1]

  logsdb, err := sql.Open("sqlite3", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=on", sqlitePath))
  if err != nil { log.Fatalf(err.Error()) }
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
  if err := migrations.Migrate(logsdb); err != nil {
    log.Fatalf(err.Error())
  }


  feed, err := datafeed.ResolveFeed(feedURL, logsdb)
  if err != nil { log.Fatalf(err.Error()) }

  quit := make(chan struct{})
  // go indexer.ProcessFeed(feed, logsdb, quit)
  go indexer.ProcessDataFeed(feed, logsdb, quit, eip155Block, homesteadBlock)


  mux := http.NewServeMux()
  mux.HandleFunc("/", flumehandler.GetHandler(logsdb))
  mux.HandleFunc("/api", flumehandler.GetAPIHandler(logsdb))
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
  if *shutdownSync {
    logsdb.Exec("VACUUM;")
  }
  logsdb.Close()
  time.Sleep(time.Second)
}
