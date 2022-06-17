package indexer

import (
	"testing"

	log "github.com/inconshreveable/log15"
	_ "github.com/mattn/go-sqlite3"
	_ "net/http/pprof"
)

func TestLogIndexer(t *testing.T) {
	controlDB, err := openControlDatabase("lg", "../../logs.sqlite")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = controlDB.Exec(`CREATE TABLE event_logs (
				address varchar(20),
				topic0 varchar(32),
				topic1 varchar(32),
				topic2 varchar(32),
				topic3 varchar(32),
				data blob,
				block BIGINT,
				logIndex MEDIUMINT,
				transactionHash varchar(32),
				transactionIndex varchar(32),
				blockHash varchar(32),
				PRIMARY KEY (block, logIndex)
			)`)
	if err != nil {
		t.Fatalf(err.Error())
	}

	batches, err := pendingBatchDecompress()
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Info("Log indexer test", "Decompressing batches of length:", len(batches))
	l := NewLogIndexer()

	statements := []string{}
	for _, pb := range batches {
		group, err := l.Index(pb)
		if err != nil {
			t.Fatalf(err.Error())
		}
		statements = append(statements, group...)
	}
	//megaStatement adds significant time to test. TODO: investigate why
	// megaStatement := strings.Join(statements, ";")
	// _, err = controlDB.Exec(megaStatement)
	// if err != nil {t.Fatalf(err.Error())}

	for i, statement := range statements {
		_, err := controlDB.Exec(statement)
		if err != nil {
			t.Fatalf("error: %v, statement: %v, index: %v", err.Error(), statement, i)
		}
	}

	query := "SELECT l.address = event_logs.address, l.topic0 = event_logs.topic0, l.topic1 = event_logs.topic1, l.topic2 = event_logs.topic2, l.topic3 = event_logs.topic3, l.data = event_logs.data, l.block = event_logs.block, l.logIndex = event_logs.logIndex, l.transactionHash = event_logs.transactionHash, l.transactionIndex = event_logs.transactionIndex, l.blockHash = event_logs.blockHash FROM event_logs INNER JOIN control.event_logs as l on event_logs.block AND event_logs.logIndex = l.block AND l.logIndex"
	results := make([]any, 11)
	for i := 0; i < len(results); i++ {
		var x bool
		results[i] = &x
	}
	rows, err := controlDB.Query(query)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(results...)
		for i, item := range results {
			if v, ok := item.(*bool); !*v || !ok {
				t.Errorf("failed on index %v, %v, %v", i, v, ok)
			}
		}
	}
}
