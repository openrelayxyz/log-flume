package indexer

import (
	"testing"
	// "strings"

	_ "github.com/mattn/go-sqlite3"
	// log "github.com/inconshreveable/log15"
)

func TestTransacitonIndexer(t *testing.T) {
	controlDB, err := openControlDatabase("tx", "../../transactions.sqlite")
	if err != nil {t.Fatalf(err.Error())}
	_, err = controlDB.Exec(`CREATE TABLE transactions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				gas BIGINT,
				gasPrice BIGINT,
				hash varchar(32) UNIQUE,
				input blob,
				nonce BIGINT,
				recipient varchar(20),
				transactionIndex MEDIUMINT,
				value varchar(32),
				v SMALLINT,
				r varchar(32),
				s varchar(32),
				sender varchar(20),
				func varchar(4),
				contractAddress varchar(20),
				cumulativeGasUsed BIGINT,
				gasUsed BIGINT,
				logsBloom blob,
				status TINYINT,
				block BIGINT,
				type TINYINT,
				access_list blob,
				gasFeeCap varchar(32),
				gasTipCap varchar(32))`)
	if err != nil {t.Fatalf(err.Error())}

	batches, err := pendingBatchDecompress()
	if err != nil {t.Fatalf(err.Error())}

	ti := NewTxIndexer(1, 2675000, 1150000)

	statements := []string{}
	for _, pb := range batches {
		// log.Info("number", "number", pb.Number)
		group, err := ti.Index(pb)
		if err != nil {t.Fatalf(err.Error())}
		statements = append(statements, group...)
	}

	// megaStatement := strings.Join(statements, ";")
	// _, err = controlDB.Exec(megaStatement)
	// if err != nil {t.Fatalf(err.Error())}

	for i, statement := range statements {
		_, err := controlDB.Exec(statement)
		if err != nil {t.Fatalf("error: %v, statement: %v, index: %v, previous %v",err.Error(), statement, i, statements[i - 1]) }
	}

	// query := "SELECT count(*) from transactions;"
	// query := "SELECT t.block = transactions.block FROM transactions INNER JOIN control.transactions as t on transactions.id = t.id"
	// var expected bool
	// if err := controlDB.QueryRow(query).Scan(&expected); err != nil{
	// 	t.Fatalf(err.Error())
	// }
	// if !expected {
	// 	t.Fatalf("expected: %v", expected)
	// }
	var expected int64
	if err := controlDB.QueryRow("SELECT count(*) from control.transactions;").Scan(&expected); err != nil{
		t.Fatalf(err.Error())
	}
	var test int64
	if err := controlDB.QueryRow("SELECT count(*) from transactions;").Scan(&test); err != nil{
		t.Fatalf("error: %v, expected: %v, test: %v", err.Error(), expected, test)
	}
	if expected != test {
		t.Fatalf("expected: %v, test: %v", expected, test)
	}
}
