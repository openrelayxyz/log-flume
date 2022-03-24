package api

import (
	"reflect"
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
	// "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	// "net/http"
	"path/filepath"
	"encoding/json"
	// "flag"
	// "fmt"
	// "time"
	// "log"
	"io/ioutil"
	"github.com/mattn/go-sqlite3"
	_ "net/http/pprof"
	"database/sql"
	"sync"
)

var farewell string = "goodbuy horses"

var register sync.Once

func connectToDatabase() (*sql.DB, error) {
  sqlitePath := "../../testdata.sqlite"
  // feedURL := "null://"

  mempoolDb := filepath.Join(filepath.Dir(sqlitePath), "mempool.sqlite")

  register.Do(func () {sql.Register("sqlite3_hooked",
    &sqlite3.SQLiteDriver{
      ConnectHook: func(conn *sqlite3.SQLiteConn) error {
        conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'mempool'; PRAGMA mempool.journal_mode = WAL ; PRAGMA mempool.synchronous = OFF ;", mempoolDb), nil)
        return nil
      },
  })})

  logsdb, err := sql.Open("sqlite3_hooked", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=on", sqlitePath))
	//we should add migrations process
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
	defer db.Close()
	b := NewBlockAPI(db, 1)
	expectedResult, _ := hexutil.DecodeUint64("0xd59f80")
	test , err:= b.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}
	if test != hexutil.Uint64(expectedResult) {
		t.Fatalf("BlockNumber() result not accurate")
	}
}

func decompressAndUnmarshal(fileString string, reciever interface{}) (interface{}, error) {
	file, err := ioutil.ReadFile(fileString)
	if err != nil {
		return nil, err
	}
	result := reciever
	json.Unmarshal([]byte(file), &reciever)
	return result, nil
}

// func TestGetBlockByNumber(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	blocksMap := []*Block{}
// 	file, _ := ioutil.ReadFile("data.json")
// 	json.Unmarshal([]byte(file), &blocksMap)
// 	for i, block := range blockNumbers[5:] {
// 			actual, err := b.GetBlockByNumber(context.Background(), block, true)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
// 			if actual != blocksMap[i + 5]{
// 				t.Errorf("Error with block %v and %v", actual, blocksMap[i])
// 			}
// 	}
// }

// var blocksMap []map[string]hexutil.Uint64

func TestJson(t *testing.T) {
		db, err := connectToDatabase()
		if err != nil {
			t.Fatal(err.Error())
		}
		defer db.Close()
		b := NewBlockAPI(db, 1)
		var blocksMap []map[string]interface{}
		raw, _ := jsonDecompress("data.json.gz")
		json.Unmarshal(raw, &blocksMap)
		actual, err := b.GetBlockByNumber(context.Background(), blockNumbers[0], true)
		if err != nil {t.Fatal(err.Error())}
		if actual["gasUsed"].(string) != blocksMap[0]["gasUsed"].(string) {
			t.Errorf("%v %v", reflect.TypeOf(actual["gasUsed"]), reflect.TypeOf(blocksMap[5]["gasUsed"]))
	}

}








// func TestGetBlockByNumber(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	// actual , err := b.GetBlockByHash(context.Background(), hash, true)
// 	// if err != nil {
// 	// 	t.Fatal(err.Error())
// 	// }
// 	// actualBytes, _ := json.Marshal(actual)
// 	// if string(actualBytes) != gBBNExpectedResults[0] {
// 	// 	t.Fatal(string(actualBytes), gBBNExpectedResults[0])
// 	// }
// 	for i, block := range blockNumbers {
//     t.Run("GetBlockByNumber", func(T *testing.T) {
// 			actual , err := b.GetBlockByNumber(context.Background(), block, true)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
// 			actualBytes, _ := json.Marshal(actual)
//       if string(actualBytes) != gBBNExpectedResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// 	for i, hash := range blockHashes {
// 		t.Run("GetBlockByHash", func(T *testing.T) {
// 			actual , err := b.GetBlockByHash(context.Background(), common.HexToHash(hash), true)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
// 			actualBytes, _ := json.Marshal(actual)
// 			if string(actualBytes) != gBBNExpectedResults[i] {
// 				t.Errorf("Error with block %v", i)
// 			}
// 		})
// 	}
// }
// func TestGetBlockTransactionCount(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	for i, block := range blockNumbers {
//     t.Run("GetBlockTransactionCountByNumber", func(T *testing.T) {
// 			actual , err := b.GetBlockTransactionCountByNumber(context.Background(), block)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != txCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// 	for i, hash := range blockHashes {
//     t.Run("GetBlockTransactionCountByHash", func(T *testing.T) {
// 			actual , err := b.GetBlockTransactionCountByHash(context.Background(), common.HexToHash(hash))
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != txCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// }
//
// func TestGetUncleCount(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	for i, block := range blockNumbers {
//     t.Run("GetUncleCountByBlockNumber", func(T *testing.T) {
// 			actual , err := b.GetUncleCountByBlockNumber(context.Background(), block)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != uncleCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// 	for i, hash := range blockHashes {
//     t.Run("GetUncleCountByBlockHash", func(T *testing.T) {
// 			actual , err := b.GetUncleCountByBlockHash(context.Background(), common.HexToHash(hash))
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != uncleCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// }
//
// func TestGetTransactions(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	tx := NewTransactionAPI(db, 1)
// 	// b := NewBlockAPI(db, 1)
//
// 	for i, block := range blockNumbers {
//     t.Run("GetTransactionByBlockNumberAndIndex", func(T *testing.T) {
// 			blockMap := Block{}
// 			json.Unmarshal([]byte(gBBNExpectedResults[i]), &blockMap)
// 			txns := blockMap.Transactions
// 			for j, _ := range txns {
// 				actual , err := tx.GetTransactionByBlockNumberAndIndex(context.Background(), block, hexutil.Uint64(j))
// 				if err != nil {
// 					t.Fatal(err.Error())
// 			}
// 			if actual != txns[j] {
// 				t.Errorf("Error with block %v %v %v %v", actual, txns[j], block, j)
// 			}
//       }
//     })
//   }
//
// 	// for i, hash := range blockHashes {
//   //   t.Run("GetUncleCountByBlockHash", func(T *testing.T) {
// 	// 		actual , err := b.GetUncleCountByBlockHash(context.Background(), common.HexToHash(hash))
// 	// 		if err != nil {
// 	// 			t.Fatal(err.Error())
// 	// 		}
//   //     if actual != uncleCountResults[i] {
//   //       t.Errorf("Error with block %v", i)
//   //     }
//   //   })
//   // }
// }
//
// // GetTransactionByBlockNumberAndIndex
//
// func TestGasPrice(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	g := NewGasAPI(db, 1)
// 	expectedResult  := "0x1f47a69b13"
// 	test , err:= g.GasPrice(context.Background())
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}
// 	if test != expectedResult {
// 		t.Fatalf("GasPrice() result not accurate")
// 	}
// }

// func TestMaxPriorityFeePerGas(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	g := NewGasAPI(db, 1)
// 	expectedResult  := "0xa84b504a"
// 	test , err:= g.MaxPriorityFeePerGas(context.Background())
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}
// 	if test != expectedResult {
// 		t.Fatalf("GasPrice() result not accurate")
// 	}
// }
