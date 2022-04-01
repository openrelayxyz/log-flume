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

func getReceiptHashMap() (map[common.Hash][]map[string]json.RawMessage, error){
	bkHashes := getBlockHashes()
	result := map[common.Hash][]map[string]json.RawMessage{}
	receiptsMap, _ := receiptsDecompress()
	for _, hash := range bkHashes {
		receipts := []map[string]json.RawMessage{}
		for _, receipt := range receiptsMap {
				var h common.Hash
				json.Unmarshal(receipt["blockHash"], &h)
				if hash == h {
					receipts = append(receipts, receipt)
					result[hash] = receipts
				}
			}
	}
	return result, nil
	}

	func getReceiptBlockMap() (map[rpc.BlockNumber][]map[string]json.RawMessage, error){
		bkNumbers := getBlockNumbers()
		result := map[rpc.BlockNumber][]map[string]json.RawMessage{}
		receiptsMap, _ := receiptsDecompress()
		for _, number := range bkNumbers {
			receipts := []map[string]json.RawMessage{}
			for _, receipt := range receiptsMap {
					var n rpc.BlockNumber
					json.Unmarshal(receipt["blockNumber"], &n)
					if number == n {
						receipts = append(receipts, receipt)
						result[number] = receipts
					}
				}
		}
		return result, nil
		}




func TestExperiment2(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	f := NewFlumeAPI(db, 1)
	bkHashes := getBlockHashes()
	bkNumbers := getBlockNumbers()
	receiptsHashMap, _ := getReceiptHashMap()
	receiptsBlockMap, _ := getReceiptBlockMap()
 	for i, hash := range bkHashes {
		t.Run(fmt.Sprintf("GetTransactionReceiptsByBlockHash %v", i), func(t *testing.T){
			actual, _ := f.GetTransactionReceiptsByBlockHash(context.Background(), hash)
			for j := range actual {
				for k, v := range actual[j]{
					data, err := json.Marshal(v)
					if err != nil {t.Errorf(err.Error())}
					if !bytes.Equal(data, receiptsHashMap[hash][j][k]) {
						t.Fatalf("experiment %v %v %v %v", k, bkHashes[11], string(data), string(receiptsHashMap[bkHashes[11]][j][k]))
					}
				}
			}
		})
	}
	for i, number := range bkNumbers {
	t.Run(fmt.Sprintf("GetTransactionReceiptsByBlockNumber%v", i), func(t *testing.T){
		actual, _ := f.GetTransactionReceiptsByBlockNumber(context.Background(), hexutil.Uint64(number))
		for j := range actual {
			for k, v := range actual[j]{
				data, err := json.Marshal(v)
				if err != nil {t.Errorf(err.Error())}
				if !bytes.Equal(data, receiptsBlockMap[number][j][k]) {
					t.Fatalf("experiment %v %v %v %v", k, bkHashes[11], string(data), string(receiptsHashMap[bkHashes[11]][j][k]))
				}
			}
		}
	})
}
}
