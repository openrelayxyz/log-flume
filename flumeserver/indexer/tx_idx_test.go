package indexer

import (
	"testing"
	// "strings"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/inconshreveable/log15"
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

	log.Info("info", "batches", len(batches))

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


	// query := "SELECT t.block, t.input, transactions.input, t.input = transactions.input FROM transactions INNER JOIN control.transactions as t on transactions.hash = t.hash WHERE t.input != transactions.input"
		// query2 := "SELECT t.block, t.transactionIndex, transactions.transactionIndex, t.transactionIndex = transactions.transactionIndex FROM transactions INNER JOIN control.transactions as t on transactions.hash = t.hash"
	// 	query3 := "SELECT t.block, t.sender, transactions.sender, t.sender = transactions.sender FROM transactions INNER JOIN control.transactions as t on transactions.hash = t.hash WHERE t.sender != transactions.sender"
	//
	// rows, err := controlDB.Query(query3)
	// if err != nil {t.Fatalf(err.Error())}
	//
	//
	// defer rows.Close()
	//
	// for rows.Next() {
	//
	// 	var number, control, test, compare interface{}
	// 	rows.Scan(&number, &control, &test, &compare)
	//
	// 	log.Info("results", "numnber", number, "control", control, "test", test, "compare", compare)
	//
	// }

	//input, nonce, recipient, transactionIndex, `value`, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, `status`, `type`, access_list, gasFeeCap, gasTipCap

	//input and logs bloom are compressed will qrite seperate test

	query := "SELECT t.block = transactions.block, t.gas = transactions.gas, t.gasPrice = transactions.gasPrice, t.hash = transactions.hash, t.nonce = transactions.nonce, t.recipient = transactions.recipient, t.transactionIndex = transactions.transactionIndex, t.value = transactions.value, t.v = transactions.v, t.r = transactions.r, t.s = transactions.s, t.sender = transactions.sender, t.func = transactions.func, t.contractAddress = transactions.contractAddress, t.cumulativeGasUsed = transactions.cumulativeGasUsed, t.gasUsed = transactions.gasUsed, t.status = transactions.status, t.type = transactions.type, t.access_list = transactions.access_list, t.gasFeeCap = transactions.gasFeeCap, t.gasTipCap = transactions.gasTipCap FROM transactions INNER JOIN control.transactions as t on transactions.hash = t.hash"

	results := make([]any, 21)
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

			if i == 12 || i == 14 || i == 15 || i == 16 || i == 17 || i == 18 || i == 19 || i == 20 || i == 21 || i == 22 {
					continue
			}
			if v, ok := item.(*bool); !*v || !ok {
				t.Errorf("failed on index %v, %v, %v", i, *v, ok)
			}
		}
	}
}
