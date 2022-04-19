package api

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openrelayxyz/flume/flumeserver/migrations"

	"compress/gzip"
	"database/sql"
	"encoding/json"
	"github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"path/filepath"
	"sync"
)

var register sync.Once

func connectToDatabase() (*sql.DB, error) {
	sqlitePath := "../../main.sqlite"

	mempoolDb := filepath.Join(filepath.Dir(sqlitePath), "mempool.sqlite")
	blocksDb := filepath.Join(filepath.Dir(sqlitePath), "blocks.sqlite")
	txDb := filepath.Join(filepath.Dir(sqlitePath), "transactions.sqlite")
	logsDb := filepath.Join(filepath.Dir(sqlitePath), "logs.sqlite")

	register.Do(func() {
		sql.Register("sqlite3_hooked",
			&sqlite3.SQLiteDriver{
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'mempool'; PRAGMA mempool.journal_mode = WAL ; PRAGMA mempool.synchronous = OFF ;", mempoolDb), nil)
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'blocks'; PRAGMA block.journal_mode = WAL ; PRAGMA block.synchronous = OFF ;", blocksDb), nil)
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'transactions'; PRAGMA transactions.journal_mode = WAL ; PRAGMA transactions.synchronous = OFF ;", txDb), nil)
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'logs'; PRAGMA logs.journal_mode = WAL ; PRAGMA logs.synchronous = OFF ;", logsDb), nil)
					return nil
				},
			})
	})

	logsdb, err := sql.Open("sqlite3_hooked", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=off", sqlitePath))

	chainid := uint64(1)

	if err := migrations.MigrateBlocks(logsdb, chainid); err != nil {
		return nil, err
	}
	if err := migrations.MigrateTransactions(logsdb, chainid); err != nil {
		return nil, err
	}
	if err := migrations.MigrateLogs(logsdb, chainid); err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}
	return logsdb, nil
}

func blocksDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("new_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blocksObject []map[string]json.RawMessage
	json.Unmarshal(raw, &blocksObject)
	return blocksObject, nil
}

func receiptsDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("new_receipts.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var receiptsObject []map[string]json.RawMessage
	json.Unmarshal(raw, &receiptsObject)
	return receiptsObject, nil
}

func getBlockNumbers(jsonBlockObject []map[string]json.RawMessage) []rpc.BlockNumber {
	result := []rpc.BlockNumber{}
	for _, block := range jsonBlockObject {
		var x rpc.BlockNumber
		json.Unmarshal(block["number"], &x)
		result = append(result, x)
	}
	return result
}

func getBlockHashes(jsonBlockObject []map[string]json.RawMessage) []common.Hash {
	result := []common.Hash{}
	for _, block := range jsonBlockObject {
		var x common.Hash
		json.Unmarshal(block["hash"], &x)
		result = append(result, x)
	}
	return result
}

func TestBlockNumber(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	b := NewBlockAPI(db, 1)
	expectedResult, _ := hexutil.DecodeUint64("0xd59f95")
	test, err := b.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}
	if test != hexutil.Uint64(expectedResult) {
		t.Fatalf("BlockNumber() result not accurate")
	}
}

func TestBlockAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	b := NewBlockAPI(db, 1)
	blockObject, _ := blocksDecompress()
	blockNumbers := getBlockNumbers(blockObject)
	for i, block := range blockNumbers {
		t.Run(fmt.Sprintf("GetBlockByNumber %v", i), func(t *testing.T) {
			actual, err := b.GetBlockByNumber(context.Background(), block, true)
			if err != nil {
				t.Fatal(err.Error())
			}
			for k, v := range actual {
				if k == "transactions" {
					txs := v.([]map[string]interface{})
					var blockTxs []map[string]json.RawMessage
					json.Unmarshal(blockObject[i]["transactions"], &blockTxs)
					for j, item := range txs {
						for key, value := range item {
							d, err := json.Marshal(value)
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
							}
							if !bytes.Equal(d, blockTxs[j][key]) {
								t.Fatalf("didnt work")
							}

						}
					}
				} else {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf("nope %v", k)
					}
					if !bytes.Equal(data, blockObject[i][k]) {
						t.Fatalf("not equal %v", k)
					}
				}
			}
		})

		t.Run("GetBlockTransactionCountByNumber", func(t *testing.T) {
			actual, err := b.GetBlockTransactionCountByNumber(context.Background(), block)
			if err != nil {
				t.Fatal(err.Error())
			}
			var txSlice []map[string]interface{}
			json.Unmarshal(blockObject[i]["transactions"], &txSlice)
			if actual != hexutil.Uint64(len(txSlice)) {
				t.Fatalf("transaction count by block %v %v", actual, hexutil.Uint64(len(txSlice)))
			}
		})

		t.Run("GetUncleCountByBlockNumber", func(t *testing.T) {
			actual, err := b.GetUncleCountByBlockNumber(context.Background(), block)
			if err != nil {
				t.Fatal(err.Error())
			}
			var uncleSlice []common.Hash
			json.Unmarshal(blockObject[i]["uncles"], &uncleSlice)
			if actual != hexutil.Uint64(len(uncleSlice)) {
				t.Fatalf("uncle count by block %v %v", actual, hexutil.Uint64(len(uncleSlice)))
			}
		})
		blockHashes := getBlockHashes(blockObject)
		for i, hash := range blockHashes {
			t.Run(fmt.Sprintf("GetBlockByHash %v", i), func(t *testing.T) {
				actual, err := b.GetBlockByHash(context.Background(), hash, true)
				if err != nil {
					t.Fatal(err.Error())
				}
				for k, v := range actual {
					if k == "transactions" {
						txs := v.([]map[string]interface{})
						var blockTxs []map[string]json.RawMessage
						json.Unmarshal(blockObject[i]["transactions"], &blockTxs)
						for j, item := range txs {
							for key, value := range item {
								d, err := json.Marshal(value)
								if err != nil {
									t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
								}
								if !bytes.Equal(d, blockTxs[j][key]) {
									t.Fatalf("didnt work")
								}

							}
						}
					} else {
						data, err := json.Marshal(v)
						if err != nil {
							t.Errorf("nope %v", k)
						}
						if !bytes.Equal(data, blockObject[i][k]) {
							t.Fatalf("not equal %v", k)
						}
					}
				}
			})
			t.Run("GetBlockTransactionCountByHash", func(t *testing.T) {
				actual, err := b.GetBlockTransactionCountByHash(context.Background(), hash)
				if err != nil {
					t.Fatal(err.Error())
				}
				var txSlice []map[string]interface{}
				json.Unmarshal(blockObject[i]["transactions"], &txSlice)
				if actual != hexutil.Uint64(len(txSlice)) {
					t.Fatalf("transaction count by hash %v %v", actual, hexutil.Uint64(len(txSlice)))
				}
			})

			t.Run("GetUncleCountByBlockHash", func(t *testing.T) {
				actual, err := b.GetUncleCountByBlockHash(context.Background(), hash)
				if err != nil {
					t.Fatal(err.Error())
				}
				var uncleSlice []common.Hash
				json.Unmarshal(blockObject[i]["uncles"], &uncleSlice)
				if actual != hexutil.Uint64(len(uncleSlice)) {
					t.Fatalf("uncle count by hash %v %v", actual, hexutil.Uint64(len(uncleSlice)))
				}
			})
		}
	}
}
