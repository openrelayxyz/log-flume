package api

import (
	"testing"
	"context"
	"fmt"
	// "os"
	// "github.com/openrelayxyz/cardinal-rpc/transports"
	// "github.com/openrelayxyz/flume/flumeserver/txfeed"
	// "github.com/openrelayxyz/flume/flumeserver/datafeed"
	// "github.com/openrelayxyz/flume/flumeserver/indexer"
	// "github.com/openrelayxyz/flume/flumeserver/migrations"
	// "github.com/openrelayxyz/flume/flumeserver/notify"
	// "github.com/openrelayxyz/flume/flumeserver/api"
	// gethLog "github.com/ethereum/go-ethereum/log"
	// "github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/common/hexutil"
	// "net/http"
	"path/filepath"
	// "flag"
	// "fmt"
	// "time"
	// "log"
	"github.com/mattn/go-sqlite3"
	_ "net/http/pprof"
	"database/sql"
	// "sync"
)

var farewell string = "goodbuy horses"

var l LogsAPI
// var b BlockAPI
var g GasAPI
var tx TransactionAPI
var f FlumeAPI
var ft FlumeTokensAPI



func connectToDatabase() (*sql.DB, error) {
  sqlitePath := "../../testdata.sqlite"
  // feedURL := "null://"

  mempoolDb := filepath.Join(filepath.Dir(sqlitePath), "mempool.sqlite")

  sql.Register("sqlite3_hooked",
    &sqlite3.SQLiteDriver{
      ConnectHook: func(conn *sqlite3.SQLiteConn) error {
        conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'mempool'; PRAGMA mempool.journal_mode = WAL ; PRAGMA mempool.synchronous = OFF ;", mempoolDb), nil)
        return nil
      },
  })

  logsdb, err := sql.Open("sqlite3_hooked", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=on", sqlitePath))
	//we should migrations process
	if err != nil {
		return nil, err
	}
	return logsdb, nil
}

func TestBlockNumber(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	b := NewBlockAPI(db, 1)
	expectedResult, _ := hexutil.DecodeUint64("0xd59f80")
	test , err:= b.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}
	if test != hexutil.Uint64(expectedResult) {
		t.Fatalf("result not accurate")
	}
}

// func TestLogMethod(t *testing.T) {
// 	var err error
//
// 	test := l.Logs()
//
// 	if test != farewell {
// 		t.Fatalf(err.Error())
// 	}
// }

// func TestBlockMethod(t *testing.T) {
// 	var err error
//
// 	test := b.Block()
//
// 	if test != farewell {
// 		t.Fatalf(err.Error())
// 	}
// }

// func TestGasMethod(t *testing.T) {
// 	var err error
//
// 	test := g.Gas()
//
// 	if test != farewell {
// 		t.Fatalf(err.Error())
// 	}
// }
//
// func TestTransactionMethod(t *testing.T) {
// 	var err error
//
// 	test := tx.Transaction()
//
// 	if test != farewell {
// 		t.Fatalf(err.Error())
// 	}
// }
//
// func TestFlumeMethod(t *testing.T) {
// 	var err error
//
// 	test := f.Flume()
//
// 	if test != farewell {
// 		t.Fatalf(err.Error())
// 	}
// }
//
// func TestFlumeTokensMethod(t *testing.T) {
// 	var err error
//
// 	test := ft.FlumeTokens()
//
// 	if test != farewell {
// 		t.Fatalf(err.Error())
// 	}
// }

// func TestGetBlockMethod(t *testing.T) {
// 	var err error
// 	var expectedResult
//
// 	test := b.BlockNumber()
//
// 	if test != expectedResult {
// 		t.Fatalf(err.Error())
// 	}
// }
