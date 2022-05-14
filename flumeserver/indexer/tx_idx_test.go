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

	query := "SELECT t.block = transactions.block FROM transactions INNER JOIN control.transactions as t on transactions.hash = t.hash"

	// query := "SELECT t.block = transactions.block, t.gas = transactions.gas, t.gasPrice = transactions.gasPrice, t.hash = transactions.hash, t.input = transactions.input, t.nonce = transactions.nonce, t.recipient = transactions.recipient, t.transactionIndex = transactions.transactionIndex, t.value = transactions.value, t.v = transactions.v, t.r = transactions.r, t.s = transactions.s, t.sender = transactions.sender, t.func = transactions.func, t.contractAddress = transactions.contractAddress, t.cumulativeGasUsed = transactions.cumulativeGasUsed, t.gasUsed = transactions.gasUsed, t.logsBloom = transactions.logsBloom, t.status = transactions.status, t.type = transactions.type, t.access_list = transactions.access_list, t.gasFeeCap = transactions.gasFeeCap, t.gasTipCap = transactions.gasTipCap FROM transactions INNER JOIN control.transactions as t on transactions.hash = t.hash"

	results := make([]any, 1)
	for i := 0; i < len(results); i++ {
		var x bool
		results[i] = &x
	}
	rows, err := controlDB.Query(query)
	if err != nil{t.Fatalf(err.Error())}
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
