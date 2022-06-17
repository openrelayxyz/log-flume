package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	log "github.com/inconshreveable/log15"
	"github.com/mattn/go-sqlite3"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
	"github.com/openrelayxyz/flume/flumeserver/api"
	"github.com/openrelayxyz/flume/flumeserver/indexer"
	"github.com/openrelayxyz/flume/flumeserver/migrations"
	"github.com/openrelayxyz/flume/flumeserver/txfeed"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-types/metrics/publishers"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"
)


func main() {
	exitWhenSynced := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
	resumptionTimestampMs := flag.Int64("resumption.ts", -1, "Timestamp (in ms) to resume from instead of database timestamp (requires Cardinal source)")

	flag.CommandLine.Parse(os.Args[1:])

	cfg, err := LoadConfig(flag.CommandLine.Args()[0])
	if err != nil {
		log.Error("Error parsing config", "err", err)
		os.Exit(1)
	}

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
	if err != nil {
		log.Error(err.Error())
	}

	logsdb.SetConnMaxLifetime(0)
	logsdb.SetMaxIdleConns(32)

	go func() {
		connectionsGauge := metrics.NewMajorGauge("/flume/connections")
		inUseGauge := metrics.NewMajorGauge("/flume/inUse")
		idleGauge := metrics.NewMajorGauge("/flume/idle")
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			stats := logsdb.Stats()
			connectionsGauge.Update(int64(stats.OpenConnections))
			inUseGauge.Update(int64(stats.InUse))
			idleGauge.Update(int64(stats.Idle))
		}
	}()
	if cfg.PprofPort > 0 {
		p := &http.Server{
			Addr:              fmt.Sprintf(":%v", cfg.PprofPort),
			Handler:           http.DefaultServeMux,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       120 * time.Second,
			MaxHeaderBytes:    1 << 20,
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
	if err != nil {
		log.Error(err.Error())
	}
	quit := make(chan struct{})
	mut := &sync.RWMutex{}

	consumer, _ := AquireConsumer(logsdb, cfg.brokers, cfg.ReorgThreshold, int64(cfg.Chainid), *resumptionTimestampMs)
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
	tm.Register("debug", &metrics.MetricsAPI{})

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
	if cfg.Statsd != nil {
		publishers.StatsD(cfg.Statsd.Port, cfg.Statsd.Address, time.Duration(cfg.Statsd.Interval), cfg.Statsd.Prefix, cfg.Statsd.Minor)
	}
	if cfg.CloudWatch != nil {
		publishers.CloudWatch(cfg.CloudWatch.Namespace, cfg.CloudWatch.Dimensions, cfg.CloudWatch.Chainid, time.Duration(cfg.CloudWatch.Interval), cfg.CloudWatch.Percentiles, cfg.CloudWatch.Minor)
	}
	quit <- struct{}{}
	logsdb.Close()
	metrics.Clear()
	time.Sleep(time.Second)
}
