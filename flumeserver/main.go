package main

import (
  "context"
  "os"
	"strings"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
  "github.com/openrelayxyz/flume/flumeserver/txfeed"
  "github.com/openrelayxyz/flume/flumeserver/indexer"
  "github.com/openrelayxyz/flume/flumeserver/migrations"
	streamsTransports "github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/flume/flumeserver/api"
	ctypes "github.com/openrelayxyz/cardinal-types"
	log "github.com/inconshreveable/log15"
	"math/big"
  "net/http"
  "flag"
  "fmt"
  "time"
  "github.com/mattn/go-sqlite3"
	"github.com/openrelayxyz/cardinal-streams/transports"
  _ "net/http/pprof"
  "database/sql"
  "sync"
	"regexp"
)

func aquire_consumer(db *sql.DB, brokerParams []transports.BrokerParams, reorgThreshold, chainid, resumptionTime int64) (streamsTransports.Consumer, error) {
	var err error
	var tableName string
	db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='cardinal_offsets';").Scan(&tableName)
	if tableName != "cardinal_offsets" {
		if _, err = db.Exec("CREATE TABLE cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition));"); err != nil {
			return nil, err
		}
	}
	startOffsets := []string{}
	for _, broker := range brokerParams {
		for _, topic := range broker.Topics {
			var partition int32
			var offset int64
			rows, err := db.QueryContext(context.Background(), "SELECT partition, offset FROM cardinal_offsets WHERE topic = ?;", topic)
			if err != nil {
		 		return nil, err}
				for rows.Next() {
					if err := rows.Scan(&partition, &offset); err != nil { return nil, err }
				startOffsets = append(startOffsets, fmt.Sprintf("%v:%v=%v", topic, partition, offset))
			}
		}
	}
	resumption := strings.Join(startOffsets, ";")
	var lastHash, lastWeight []byte
	var lastNumber int64
	db.QueryRowContext(context.Background(), "SELECT max(number), hash, td FROM blocks;").Scan(&lastNumber, &lastHash, &lastWeight)

	trackedPrefixes := []*regexp.Regexp{
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/h"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/d"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/u"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/t/"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/r/"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/l/"),
	}
	log.Info("Resuming to block", "number", lastNumber)
	rt := []byte(resumption)
	if resumptionTime > 0 {
		r, err := streamsTransports.ResumptionForTimestamp(brokerParams, resumptionTime)
		if err != nil {
			log.Warn("Could not load resumption from timestamp:", "error", err.Error())
		} else {
			rt = r
		}
	}
  return streamsTransports.ResolveMuxConsumer(brokerParams, rt, lastNumber, ctypes.BytesToHash(lastHash), new(big.Int).SetBytes(lastWeight), reorgThreshold, trackedPrefixes, nil)
}

func main() {
  exitWhenSynced := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
	resumptionTimestampMs := flag.Int64("resumption.ts", -1, "Timestamp (in ms) to resume from instead of database timestamp (requires Cardinal source)")

  flag.CommandLine.Parse(os.Args[1:])

	cfg, err := LoadConfig(flag.CommandLine.Args()[0])
		if err != nil {
			log.Error("Error parsing config", "err", err)
			os.Exit(1)
		}

  // sqlitePath := flag.CommandLine.Args()[1]
  // feedURL := flag.CommandLine.Args()[2]
	//
	// if cfg.MempoolDb == "" {
	// 	cfg.MempoolDb = filepath.Join(filepath.Dir(sqlitePath), "mempool.sqlite")
	// 	log.Debug("the location of mempool is:", "mempoolDB", cfg.MempoolDb)
	// }
	// if cfg.BlocksDb == "" {
	// 	cfg.BlocksDb = filepath.Join(filepath.Dir(sqlitePath), "blocks.sqlapproriateite")
	// 	log.Debug("the location of blocksDB is:", "blocksDB", cfg.BlocksDb)
	// }
	// if cfg.TxDb == "" {
	// 	cfg.TxDb = filepath.Join(filepath.Dir(sqlitePath), "transactions.sqlite")
	// 	log.Debug("the location of transactionsDB is:", "txDB", cfg.TxDb)
	// }
	// if cfg.LogsDb == "" {
	// 	cfg.LogsDb = filepath.Join(filepath.Dir(sqlitePath), "logs.sqlite")
	// 	log.Debug("the location of logsDB is:", "logsDB", cfg.LogsDb)
	// }

  sql.Register("sqlite3_hooked",
    &sqlite3.SQLiteDriver{
      ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'blocks'; PRAGMA block.journal_mode = WAL ; PRAGMA block.synchronous = OFF ;", cfg.BlocksDb), nil)
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'transactions'; PRAGMA transactions.journal_mode = WAL ; PRAGMA transactions.synchronous = OFF ;", cfg.TxDb), nil)
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'logs'; PRAGMA logs.journal_mode = WAL ; PRAGMA logs.synchronous = OFF ;", cfg.LogsDb), nil)
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'mempool'; PRAGMA mempool.journal_mode = WAL ; PRAGMA mempool.synchronous = OFF ;", cfg.MempoolDb), nil)

				return nil
      },
  })
  logsdb, err := sql.Open("sqlite3_hooked", (":memory:?_sync=0&_journal_mode=WAL&_foreign_keys=off"))
  if err != nil { log.Error(err.Error()) }

  logsdb.SetConnMaxLifetime(0)
  logsdb.SetMaxIdleConns(32)
  go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
      stats := logsdb.Stats()
      var block uint
      logsdb.QueryRow("SELECT max(number) FROM blocks;").Scan(&block)
      log.Info("SQLite Pool", "Open:", stats.OpenConnections, "InUse:",  stats.InUse, "Idle:", stats.Idle, "Head Block:", block)
			// info becomes several metric sets, probably gauges
		}
	}()
  if cfg.PprofPort > 0 {
    p := &http.Server{
      Addr: fmt.Sprintf(":%v", cfg.PprofPort),
      Handler: http.DefaultServeMux,
      ReadHeaderTimeout: 5 * time.Second,
      IdleTimeout: 120 * time.Second,
      MaxHeaderBytes: 1 << 20,
    }
    go p.ListenAndServe()
  }

	if err := migrations.MigrateBlocks(logsdb, cfg.Chainid); err != nil {
		log.Error(err.Error())
	}
	if err := migrations.MigrateTransactions(logsdb, cfg.Chainid); err != nil {
		log.Error(err.Error())
	}
	if err := migrations.MigrateLogs(logsdb, cfg.Chainid); err != nil {
		log.Error(err.Error())
	}
	if err := migrations.MigrateMempool(logsdb, cfg.Chainid); err != nil {
		log.Error(err.Error())
	}

  txFeed, err := txfeed.ResolveTransactionFeed(cfg.brokers[0].URL, cfg.TxTopic) //does txTopic need to be addressed?
  if err != nil { log.Error(err.Error()) }
  quit := make(chan struct{})
  mut := &sync.RWMutex{}

	consumer, _ := aquire_consumer(logsdb, cfg.brokers, cfg.ReorgThreshold, int64(cfg.Chainid), *resumptionTimestampMs)
	indexes := []indexer.Indexer{
		indexer.NewBlockIndexer(cfg.Chainid),
		indexer.NewTxIndexer(cfg.Chainid, cfg.Eip155Block, cfg.HomesteadBlock),
		indexer.NewLogIndexer(),
	}
  go indexer.ProcessDataFeed(consumer, txFeed, logsdb, quit, cfg.Eip155Block, cfg.HomesteadBlock, mut, cfg.MempoolSlots, indexes) //[]indexer
	tm := rpcTransports.NewTransportManager(cfg.Concurrency)
	tm.AddHTTPServer(cfg.Port)
	tm.Register("eth", api.NewLogsAPI(logsdb, cfg.Chainid))
	tm.Register("eth", api.NewBlockAPI(logsdb, cfg.Chainid))
	tm.Register("eth", api.NewGasAPI(logsdb, cfg.Chainid))
	tm.Register("eth", api.NewTransactionAPI(logsdb, cfg.Chainid))
	tm.Register("flume", api.NewFlumeTokensAPI(logsdb, cfg.Chainid))
	tm.Register("flume", api.NewFlumeAPI(logsdb, cfg.Chainid))

  <-consumer.Ready()
  var minBlock int
  logsdb.QueryRowContext(context.Background(), "SELECT min(block) FROM event_logs;").Scan(&minBlock)
  if minBlock > cfg.MinSafeBlock {
    log.Error("Minimum block error", "Earliest log found on block:", minBlock, "Should be less than or equal to:", cfg.MinSafeBlock)
  }
  if !*exitWhenSynced {
		if err := tm.Run(9999); err != nil {
			quit <- struct{}{}
		  logsdb.Close()
		  time.Sleep(time.Second)
			os.Exit(1)
		}

  }
  quit <- struct{}{}
  logsdb.Close()
  time.Sleep(time.Second)
}
