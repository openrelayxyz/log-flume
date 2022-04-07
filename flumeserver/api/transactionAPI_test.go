package api

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	_ "net/http/pprof"
)

func getTransactionsForTesting(blockObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	result := []map[string]json.RawMessage{}
	for _, block := range blockObject {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(block["transactions"], &txns)
		result = append(result, txns...)
	}
	return result
}

func getTransactionsListsForTesting(blockObject []map[string]json.RawMessage) [][]map[string]json.RawMessage {
	result := [][]map[string]json.RawMessage{}
	for _, block := range blockObject {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(block["transactions"], &txns)
		result = append(result, txns)
	}
	return result
}

func getTransactionHashes(blockObject []map[string]json.RawMessage) []common.Hash {
	result := []common.Hash{}
	for _, block := range blockObject {
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

func getSenderAddreses(blockObject []map[string]json.RawMessage) []common.Address {
	result := []common.Address{}
	for _, block := range blockObject {
		txnLevel := []map[string]interface{}{}
		json.Unmarshal(block["transactions"], &txnLevel)
		if len(txnLevel) > 0 {
			for _, tx := range txnLevel {
				result = append(result, common.HexToAddress(tx["from"].(string)))
			}
		}
	}
	return result
}

func removeDuplicateValues(addressSlice []common.Address) []common.Address {
	keys := make(map[common.Address]bool)
	list := []common.Address{}

	for _, entry := range addressSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func TestTransactionAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	tx := NewTransactionAPI(db, 1)
	blockObject, _ := blocksDecompress()
	receiptsMap, _ := receiptsDecompress()
	transactionLists := getTransactionsListsForTesting(blockObject)
	transactions := getTransactionsForTesting(blockObject)
	txHashes := getTransactionHashes(blockObject)

	for i, hash := range txHashes {
		t.Run(fmt.Sprintf("GetTransactionByHash %v", i), func(t *testing.T) {
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
		t.Run(fmt.Sprintf("GetTransactionReceipt%v", i), func(t *testing.T) {
			actual, _ := tx.GetTransactionReceipt(context.Background(), hash)
			for k, v := range actual {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, receiptsMap[i][k]) {
					t.Fatalf("receipts error %v %v %v %v", i, k, string(data), string(receiptsMap[i+7][k]))
				}
			}
		})
	}
	for i, block := range blockObject {
		t.Run(fmt.Sprintf("GetTransactionByBlockHashAndIndex %v", i), func(t *testing.T) {
			var h common.Hash
			json.Unmarshal(block["hash"], &h)
			for j := range transactionLists[i] {
				actual, _ := tx.GetTransactionByBlockHashAndIndex(context.Background(), h, hexutil.Uint64(j))
				for k, v := range actual {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, transactionLists[i][j][k]) {
						t.Fatalf("error on blockHash%v, transaction%v, key%v", h, j, k)
					}
				}
			}
		})
		t.Run(fmt.Sprintf("GetTransactionByBlockNumberAndIndex %v", i), func(t *testing.T) {
			var n rpc.BlockNumber
			json.Unmarshal(block["number"], &n)
			for j := range transactionLists[i] {
				actual, _ := tx.GetTransactionByBlockNumberAndIndex(context.Background(), n, hexutil.Uint64(j))
				for k, v := range actual {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, transactionLists[i][j][k]) {
						var x interface{}
						json.Unmarshal(transactionLists[i][j][k], &x)
						t.Fatalf("error on block%v, transaction%v, key%v", i, j, k)
					}
				}
			}
		})
	}
	nonces := make(map[common.Address]hexutil.Uint64)
	for _, tx := range transactions {
		var sender common.Address
		json.Unmarshal(tx["from"], &sender)
		nonces[sender]++
	}

	for sender, nonce := range nonces {
		t.Run(fmt.Sprintf("GetTransactionCount"), func(t *testing.T) {
			actual, _ := tx.GetTransactionCount(context.Background(), sender)
			if actual != nonce {
				t.Fatalf("GetTransactionCountError %v %v", actual, nonce)
			}
		})
	}
}
