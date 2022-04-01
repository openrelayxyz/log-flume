package api

import (
	// "reflect"
	"testing"
	"context"
	"fmt"
	"bytes"
	// "cmp"

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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	// "github.com/google/go-cmp/cmp"v
	// "net/http"
	// "path/filepath"
	"encoding/json"
	// "flag"
	// "fmt"
	// "time"
	// "log"
	// "os"
	// "io/ioutil"
	// "github.com/mattn/go-sqlite3"
	_ "net/http/pprof"
	// "database/sql"
	// "sync"
)



func getTransactionsForTesting() []map[string]json.RawMessage {
	blocksMap, _  := jsonDecompress()
	result := []map[string]json.RawMessage{}
	for _, block := range blocksMap {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(block["transactions"], &txns)
		result = append(result, txns...)
	}
	return result
}

func getTransactionsListsForTesting() [][]map[string]json.RawMessage {
	blocksMap, _  := jsonDecompress()
	result := [][]map[string]json.RawMessage{}
	for _, block := range blocksMap {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(block["transactions"], &txns)
		result = append(result, txns)
	}
	return result
}

func getTransactionHashes() []common.Hash {
	blocksMap, _  := jsonDecompress()
	result := []common.Hash{}
	for _, block := range blocksMap {
		txnLevel := []map[string]interface{}{}
		json.Unmarshal(block["transactions"], &txnLevel)
		if len(txnLevel) > 0 {
			for _, tx := range txnLevel {
				result = append(result, common.HexToHash(tx["hash"].(string)))
			}
		}
	}
	return result
}


func TestTransactionAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	tx := NewTransactionAPI(db, 1)
	blocksMap, _  := jsonDecompress()
	// blockHashes := getBlockHashes()
	// txIndexes := getTransactionIndexes()
	receiptsMap, _  := receiptsDecompress()
	transactionLists := getTransactionsListsForTesting()
	transactions := getTransactionsForTesting()
	txHashes := getTransactionHashes()

	for i, hash := range txHashes {
		t.Run(fmt.Sprintf("GetTransactionByHash %v", i), func(t *testing.T){
			actual, err := tx.GetTransactionByHash(context.Background(), hash)
			if err != nil {
				t.Fatal(err.Error())
			}
			for k, v := range actual {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf("marshalling error gtbh on key: %v", k)
				}
				if !bytes.Equal(data, transactions[i][k]) {
					t.Fatalf("error on transaction%v, key%v", hash, k)
				}
			}
		})
		t.Run(fmt.Sprintf("GetTransactionReceipt%v", i), func(t *testing.T){
			actual, _ := tx.GetTransactionReceipt(context.Background(), hash)
			for k, v := range actual{
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, receiptsMap[i][k]){
					t.Fatalf("receipts error %v %v %v %v", i, k, string(data), string(receiptsMap[i + 7][k]))
				}
			}
		})
	}
	for i, block := range blocksMap{
		t.Run(fmt.Sprintf("GetTransactionByBlockHashAndIndex %v", i), func(t *testing.T){
			var h common.Hash
			json.Unmarshal(block["hash"], &h)
			for j := range transactionLists[i] {
				actual, _ := tx.GetTransactionByBlockHashAndIndex(context.Background(), h, hexutil.Uint64(j))
				for k, v := range actual{
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, transactionLists[i][j][k]){
						// var x interface{}
						// json.Unmarshal(transactionLists[i][j][k], &x)
						t.Fatalf("error on blockHash%v, transaction%v, key%v", h, j, k)
					}
				}
			}
		})
		t.Run(fmt.Sprintf("GetTransactionByBlockNumberAndIndex %v", i), func(t *testing.T){
			var n rpc.BlockNumber
			json.Unmarshal(block["number"], &n)
			for j := range transactionLists[i] {
				actual, _ := tx.GetTransactionByBlockNumberAndIndex(context.Background(), n, hexutil.Uint64(j))
				for k, v := range actual{
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, transactionLists[i][j][k]){
						var x interface{}
						json.Unmarshal(transactionLists[i][j][k], &x)
						t.Fatalf("error on block%v, transaction%v, key%v", i, j, k)
					}
				}
			}
		})
  }
}
// func TestExperiment(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	tx := NewTransactionAPI(db, 1)
// 	// blocksMap, _ = jsonDecompress()
// 	txHashes := getTransactionHashes()
// 	receiptsMap, _  := receiptsDecompress()
// 	for i, hash := range txHashes {
// 			actual, _ := tx.GetTransactionReceipt(context.Background(), hash)
// 			for k, v := range actual{
// 				data, err := json.Marshal(v)
// 				if err != nil {
// 					t.Errorf(err.Error())
// 				}
// 				if !bytes.Equal(data, receiptsMap[i + 7][k]){
// 					// var x interface{}
// 					// json.Unmarshal(transactionLists[i][j][k], &x)
// 					t.Fatalf("receipts error %v %v %v %v", i, k, string(data), string(receiptsMap[i + 7][k]))
// 				}
// 			}
//
// 		}
//
// }
