package main

import (
  "context"
  "os"
	"strings"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
  "github.com/openrelayxyz/flume/flumeserver/txfeed"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "github.com/openrelayxyz/flume/flumeserver/indexer"
  "github.com/openrelayxyz/flume/flumeserver/migrations"
	streamsTransports "github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/flume/flumeserver/api"
	ctypes "github.com/openrelayxyz/cardinal-types"
	log "github.com/inconshreveable/log15"
	"math/big"
  "net/http"
  "path/filepath"
  "flag"
  "fmt"
  "time"
  "github.com/mattn/go-sqlite3"
  _ "net/http/pprof"
  "database/sql"
  "sync"
	"regexp"
)

func aquire_consumer(db *sql.DB, brokerURL string, rollback, reorgThreshold, chainid, resumptionTime int64) (streamsTransports.Consumer, error) {
	var err error
	// if whitelist == nil { whitelist = make(map[uint64]ctypes.Hash) }
	var tableName string
	//needs to be revisited in terms of what db this table goes into
	//this need to changed to look into blocks db, on a seperate commit.
	db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='cardinal_offsets';").Scan(&tableName)
	if tableName != "cardinal_offsets" {
		if _, err = db.Exec("CREATE TABLE cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition));"); err != nil {
			return nil, err
		}
	}
	parts := strings.Split(brokerURL, ";")
	topics := strings.Split(parts[1], ",")
	startOffsets := []string{}
	//will likely go away in the refactor
	for _, topic := range topics {
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
	var consumer streamsTransports.Consumer
	log.Info("Resuming to block", "number", lastNumber)
	if strings.HasPrefix(parts[0], "kafka://") {
		rt := []byte(resumption)
		if resumptionTime > 0 {
			r, err := streamsTransports.ResumptionForTimestamp([]streamsTransports.BrokerParams{
				{URL: brokerURL, Topics: topics},
				}, resumptionTime)
			if err != nil {
				log.Warn("Could not load resumption from timestamp:", "error", err.Error())
			} else {
				rt = r
			}
		}
		consumer, err = streamsTransports.NewKafkaConsumer(parts[0], topics[0], topics, rt, rollback, lastNumber, ctypes.BytesToHash(lastHash), new(big.Int).SetBytes(lastWeight), reorgThreshold, trackedPrefixes, nil)
	} else if strings.HasPrefix(parts[0], "null://") {
		consumer = streamsTransports.NewNullConsumer()
	}
	return consumer, nil
}


func main() {

  exitWhenSynced := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
	resumptionTimestampMs := flag.Int64("resumption.ts", -1, "Timestamp (in ms) to resume from instead of database timestamp (requires Cardinal source)")

  port := flag.Int64("port", 8000, "Serving port")
  pprofPort := flag.Int("pprof-port", 6969, "pprof port")
  minSafeBlock := flag.Int("min-safe-block", 1000000, "Do not start serving if the smallest block exceeds this value")

	homesteadBlockFlag := flag.Int("homestead", 0, "Block of the homestead hardfork")
  eip155BlockFlag := flag.Int("eip155", 0, "Block of the eip155 hardfork")

	txTopic := flag.String("mempool-topic", "", "A kafka topic for receiving pending transactions")

	kafkaRollback := flag.Int64("kafka-rollback", 5000, "A number of Kafka offsets to roll back before resumption")
  reorgThreshold := flag.Int64("reorg-threshold", 128, "Minimum number of blocks to keep in memory to handle reorgs.")

	mempoolDb := flag.String("mempool-db", "", "A location for the mempool database (default: same dir as main db)")
	blocksDb := flag.String("blocks-db", "", "A location for the mempool database (default: same dir as main db)")
	txDb := flag.String("transactions-db", "", "A location for the mempool database (default: same dir as main db)")
	logsDb := flag.String("logs-db", "", "A location for the mempool database (default: same dir as main db)")

	mempoolSlots := flag.Int("mempool-size", 4096, "Number of mempool entries before low priced entries get dropped")


	concurrency :=flag.Int("concurrency", 16, "Number of concurrent requests to handle")

  flag.CommandLine.Parse(os.Args[1:])

	cfg, err := LoadConfig(flag.CommandLine.Args()[0])
		if err != nil {
			log.Error("Error parsing config", "err", err)
			os.Exit(1)
		}

  sqlitePath := flag.CommandLine.Args()[1]
  feedURL := flag.CommandLine.Args()[2]

	if cfg.mempoolDb == "" {
		mempoolDb = filepath.Join(filepath.Dir(sqlitePath), "mempool.sqlite")
		log.Debug("the location of mempool is:", "mempoolDB", cfg.mempoolDb)
	}
	if cfg.blocksDb == "" {
		blocksDb = filepath.Join(filepath.Dir(sqlitePath), "blocks.sqlapproriateite")
		log.Debug("the location of blocksDB is:", "blocksDB", cfg.blocksDb)
	}
	if cfg.txDb == "" {
		txDb = filepath.Join(filepath.Dir(sqlitePath), "transactions.sqlite")
		log.Debug("the location of transactionsDB is:", "txDB", cfg.txDb)
	}
	if cfg.logsDb == "" {
		logsDb = filepath.Join(filepath.Dir(sqlitePath), "logs.sqlite")
		log.Debug("the location of logsDB is:", "logsDB", cfg.logsDb)
	}

  sql.Register("sqlite3_hooked",
    &sqlite3.SQLiteDriver{
      ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'blocks'; PRAGMA block.journal_mode = WAL ; PRAGMA block.synchronous = OFF ;", cfg.blocksDb), nil)
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'transactions'; PRAGMA transactions.journal_mode = WAL ; PRAGMA transactions.synchronous = OFF ;", cfg.txDb), nil)
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'logs'; PRAGMA logs.journal_mode = WAL ; PRAGMA logs.synchronous = OFF ;", cfg.logsDb), nil)
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'mempool'; PRAGMA mempool.journal_mode = WAL ; PRAGMA mempool.synchronous = OFF ;", cfg.mempoolDb), nil)

				return nil
      },
  })
  logsdb, err := sql.Open("sqlite3_hooked", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=off", sqlitePath))
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
    }
//evetnually will ve replaced by a metrics library
	}()
  if *pprofPort > 0 {
    p := &http.Server{
      Addr: fmt.Sprintf(":%v", *pprofPort),
      Handler: http.DefaultServeMux,
      ReadHeaderTimeout: 5 * time.Second,
      IdleTimeout: 120 * time.Second,
      MaxHeaderBytes: 1 << 20,
    }
    go p.ListenAndServe()
  }

	if err := migrations.MigrateBlocks(logsdb, cfg.chainid); err != nil {
		log.Error(err.Error())
	}
	if err := migrations.MigrateTransactions(logsdb, cfg.chainid); err != nil {
		log.Error(err.Error())
	}
	if err := migrations.MigrateLogs(logsdb, cfg.chainid); err != nil {
		log.Error(err.Error())
	}
	if err := migrations.MigrateMempool(logsdb, cfg.chainid); err != nil {
		log.Error(err.Error())
	}

  feed, err := datafeed.ResolveFeed(feedURL, logsdb, cfg.kafkaRollback, cfg.reorgThreshold, cfg.chainid, *resumptionTimestampMs)
  if err != nil { log.Error(err.Error()) }
//genereic feed blocks logs tx receipts
  txFeed, err := txfeed.ResolveTransactionFeed(feedURL, cfg.txTopic) //does txTopic need to be addressed?
  if err != nil { log.Error(err.Error()) }
//mempool transactions
  quit := make(chan struct{})
  // go indexer.ProcessFeed(feed, logsdb, quit)
  mut := &sync.RWMutex{}

	var rollback, resumptionTime int64
	//TODO: the above is not ok. We need to set those variables, as flags I am assuming.
	consumer, _ := aquire_consumer(logsdb, feedURL, rollback, *reorgThreshold, int64(chainid), resumptionTime)


	//copy paste from cardinal newcardinal datafeed to get down to consumer to pass into process datafeed (see above)
	indexes := []indexer.Indexer{}
	bi := indexer.NewBlockIndexer(cfg.chainid)
	ti := indexer.NewTxIndexer(cfg.chainid, cfg.eip155Block, cfg.homesteadBlock)
	li := indexer.NewLogIndexer()
	indexes = append(indexes, bi, ti, li)
  go indexer.ProcessDataFeed(feed, consumer, txFeed, logsdb, quit, cfg.eip155Block, cfg.homesteadBlock, mut, *mempoolSlots, indexes) //[]indexer
//completion feed not really used intended to give info about when flume finished processing a block, need to strip, there may be a case for flume to offer subscritions and so may need a completion type of object (post refactor)
	tm := rpcTransports.NewTransportManager(*concurrency)
	tm.AddHTTPServer(*port)
	tm.Register("eth", api.NewLogsAPI(logsdb, cfg.chainid))
	tm.Register("eth", api.NewBlockAPI(logsdb, cfg.chainid))
	tm.Register("eth", api.NewGasAPI(logsdb, cfg.chainid))
	tm.Register("eth", api.NewTransactionAPI(logsdb, cfg.chainid))
	tm.Register("flume", api.NewFlumeTokensAPI(logsdb, cfg.chainid))
	tm.Register("flume", api.NewFlumeAPI(logsdb, cfg.chainid))



  <-feed.Ready()
	//datafeed will fire message on ready channel when caught up on latest message, t manager was set up above byt not running yet
  // if *completionTopic != "" {
  //   notify.SendKafkaNotifications(completionFeed, *completionTopic)
  // } //will be stripped
  var minBlock int
  logsdb.QueryRowContext(context.Background(), "SELECT min(block) FROM event_logs;").Scan(&minBlock)
  if minBlock > *minSafeBlock {
    log.Error("Minimum block error", "Earliest log found on block:", minBlock, "Should be less than or equal to:", *minSafeBlock)
  }
  if !*shutdownSync {
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
