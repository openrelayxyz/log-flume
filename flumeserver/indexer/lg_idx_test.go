package indexer

import (
	"testing"

	// log "github.com/inconshreveable/log15"
	_ "github.com/mattn/go-sqlite3"
	_ "net/http/pprof"
)

func TestLogIndexer(t *testing.T) {
	controlDB, err := openControlDatabase("lg", "../../logs.sqlite")
	if err != nil {t.Fatalf(err.Error())}
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
				PRIMARY KEY (transactionHash)
			)`)
	if err != nil {t.Fatalf(err.Error())}

	batches, err := pendingBatchDecompress()
	if err != nil {t.Fatalf(err.Error())}

	l := NewLogIndexer()

	statements := []string{}
	for _, pb := range batches {
		group, err := l.Index(pb)
		if err != nil {t.Fatalf(err.Error())}
		statements = append(statements, group...)
	}

	for i, statement := range statements {
		_, err := controlDB.Exec(statement)
		// log.Info("statement", "st", statement)
		if err != nil {t.Fatalf("error: %v, statement: %v, index: %v",err.Error(), statement, i) }
	}

	// query := "SELECT max(block) from event_logs;"
	var expected int64
	if err := controlDB.QueryRow("SELECT max(block) from control.event_logs").Scan(&expected); err != nil{
		t.Fatalf(err.Error())
	}
	var test int64
	if err := controlDB.QueryRow("SELECT max(block) from event_logs").Scan(&test); err != nil{
		t.Fatalf("error: %v, expected: %v, test: %v", err.Error(), expected, test)
	}
	if expected != test {
		t.Fatalf("expected: %v, test: %v", expected, test)
	}
}
