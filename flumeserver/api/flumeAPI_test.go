package api

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"encoding/json"
	_ "net/http/pprof"
)

func getHashReceipts(jsonBlockObject, jsonReceiptObject []map[string]json.RawMessage) map[common.Hash][]map[string]json.RawMessage {
	bkHashes := getBlockHashes(jsonBlockObject)
	result := map[common.Hash][]map[string]json.RawMessage{}
	for _, hash := range bkHashes {
		receipts := []map[string]json.RawMessage{}
		for _, receipt := range jsonReceiptObject {
			var h common.Hash
			json.Unmarshal(receipt["blockHash"], &h)
			if hash == h {
				receipts = append(receipts, receipt)
				result[hash] = receipts
			}
		}
	}
	return result
}

func getBlockReceipts(jsonBlockObject, jsonReceiptObject []map[string]json.RawMessage) map[rpc.BlockNumber][]map[string]json.RawMessage {
	bkNumbers := getBlockNumbers(jsonBlockObject)
	result := map[rpc.BlockNumber][]map[string]json.RawMessage{}
	for _, number := range bkNumbers {
		receipts := []map[string]json.RawMessage{}
		for _, receipt := range jsonReceiptObject {
			var n rpc.BlockNumber
			json.Unmarshal(receipt["blockNumber"], &n)
			if number == n {
				receipts = append(receipts, receipt)
				result[number] = receipts
			}
		}
	}
	return result
}

func getSenderTransactionList(jsonBlockObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	transactions := getTransactionsForTesting(jsonBlockObject)
	addr, _ := json.Marshal("0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5")
	for _, tx := range transactions {
		if bytes.Equal(tx["from"], addr) {
			results = append(results, tx)
		}
	}
	return results
}

func getRecipientTransactionList(jsonBlockObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	transactions := getTransactionsForTesting(jsonBlockObject)
	addr, _ := json.Marshal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d")
	for _, tx := range transactions {
		if bytes.Equal(tx["to"], addr) {
			results = append(results, tx)
		}
	}
	return results
}

func getSenderReceiptList(jsonReceiptObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	addr, _ := json.Marshal("0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5")
	for _, receipt := range jsonReceiptObject {
		if bytes.Equal(receipt["from"], addr) {
			results = append(results, receipt)
		}
	}
	return results
}

func getRecieverReceiptList(jsonReceiptObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	addr, _ := json.Marshal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d")
	for _, receipt := range jsonReceiptObject {
		if bytes.Equal(receipt["to"], addr) {
			results = append(results, receipt)
		}
	}
	return results
}

func TestFumeAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	f := NewFlumeAPI(db, 1)

	blockObject, _ := blocksDecompress()
	receiptObject, _ := receiptsDecompress()

	bkHashes := getBlockHashes(blockObject)
	bkNumbers := getBlockNumbers(blockObject)

	receiptsByHash := getHashReceipts(blockObject, receiptObject)
	receiptsByBlock := getBlockReceipts(blockObject, receiptObject)

	for i, hash := range bkHashes {
		t.Run(fmt.Sprintf("GetTransactionReceiptsByBlockHash %v", i), func(t *testing.T) {
			actual, _ := f.GetTransactionReceiptsByBlockHash(context.Background(), hash)
			for j := range actual {
				for k, v := range actual[j] {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, receiptsByHash[hash][j][k]) {
						t.Fatalf("experiment %v %v %v %v", k, bkHashes[11], string(data), string(receiptsByHash[bkHashes[11]][j][k]))
					}
				}
			}
		})
	}
	for i, number := range bkNumbers {
		t.Run(fmt.Sprintf("GetTransactionReceiptsByBlockNumber%v", i), func(t *testing.T) {
			actual, _ := f.GetTransactionReceiptsByBlockNumber(context.Background(), hexutil.Uint64(number))
			for j := range actual {
				for k, v := range actual[j] {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, receiptsByBlock[number][j][k]) {
						t.Fatalf("experiment %v %v %v %v", k, bkHashes[11], string(data), string(receiptsByHash[bkHashes[11]][j][k]))
					}
				}
			}
		})
	}
	sender := common.HexToAddress("0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5")
	senderTxns := getSenderTransactionList(blockObject)
	if len(senderTxns) != 47 {
		t.Fatalf("sender transactions list failed to load")
	}
	t.Run(fmt.Sprintf("GetTransactionsBySender"), func(t *testing.T) {
		actual, _ := f.GetTransactionsBySender(context.Background(), sender, 0)
		if len(actual.Items) != len(senderTxns) {
			t.Fatalf("expected %v got %v", len(actual.Items), len(senderTxns))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, senderTxns[i][k]) {
					t.Fatalf("Txns by sender error")
				}
			}
		}
	})
	t.Run(fmt.Sprintf("GetTransactionsByParticipant sender"), func(t *testing.T) {
		actual, _ := f.GetTransactionsByParticipant(context.Background(), sender, 0)
		if len(actual.Items) != len(senderTxns) {
			t.Fatalf("expected %v got %v", len(actual.Items), len(senderTxns))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, senderTxns[i][k]) {
					t.Fatalf("Txns by sender error")
				}
			}
		}
	})
	senderReceipts := getSenderReceiptList(receiptObject)
	if len(senderReceipts) != 47 {
		t.Fatalf("length %v", len(senderReceipts))
	}
	t.Run(fmt.Sprintf("GetTransactionReceiptsBySender"), func(t *testing.T) {
		actual, _ := f.GetTransactionReceiptsBySender(context.Background(), sender, 0)
		if len(actual.Items) != len(senderReceipts) {
			t.Fatalf("expected %v got %v", len(senderReceipts), len(actual.Items))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, senderReceipts[i][k]) {
					t.Fatalf("not today satan")
				}
			}
		}
	})
	t.Run(fmt.Sprintf("GetTransactionReceiptsByParticipant"), func(t *testing.T) {
		actual, _ := f.GetTransactionReceiptsByParticipant(context.Background(), sender, 0)
		if len(actual.Items) != len(senderReceipts) {
			t.Fatalf("expected %v got %v", len(senderReceipts), len(actual.Items))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, senderReceipts[i][k]) {
					t.Fatalf("not today satan")
				}
			}
		}
	})
	recipient := common.HexToAddress("0x7a250d5630b4cf539739df2c5dacb4c659f2488d")
	recipientTxns := getRecipientTransactionList(blockObject)
	if len(recipientTxns) != 107 {
		t.Fatalf("reciever transactions list failed recipient load")
	}
	t.Run(fmt.Sprintf("GetTransactionsByRecipient"), func(t *testing.T) {
		actual, _ := f.GetTransactionsByRecipient(context.Background(), recipient, 0)
		if len(actual.Items) != len(recipientTxns) {
			t.Fatalf("expected %v got %v", len(actual.Items), len(recipientTxns))
		}
		for i, tx := range actual.Items {

			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, recipientTxns[i][k]) {
					t.Fatalf("Txns by reciever error %v %v %v", i, tx["hash"], string(recipientTxns[i]["hash"]))
				}
			}
		}
	})
	t.Run(fmt.Sprintf("GetTransactionsByParicipant"), func(t *testing.T) {
		actual, _ := f.GetTransactionsByParticipant(context.Background(), recipient, 0)
		if len(actual.Items) != len(recipientTxns) {
			t.Fatalf("expected %v got %v", len(actual.Items), len(recipientTxns))
		}
		for i, tx := range actual.Items {

			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, recipientTxns[i][k]) {
					t.Fatalf("Txns by reciever error %v %v %v", i, tx["hash"], string(recipientTxns[i]["hash"]))
				}
			}
		}
	})
	recieverReceipts := getRecieverReceiptList(receiptObject)
	if len(recieverReceipts) != 107 {
		t.Fatalf("length %v", len(recieverReceipts))
	}
	t.Run(fmt.Sprintf("GetTransactionsReceiptsByRecipient"), func(t *testing.T) {
		actual, _ := f.GetTransactionReceiptsByRecipient(context.Background(), recipient, 0)
		if len(actual.Items) != len(recieverReceipts) {
			t.Fatalf("expected %v got %v", len(recieverReceipts), len(actual.Items))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, recieverReceipts[i][k]) {
					t.Fatalf("not today satan")
				}
			}
		}
	})
	t.Run(fmt.Sprintf("GetTransactionsReceiptsByParticipant"), func(t *testing.T) {
		actual, _ := f.GetTransactionReceiptsByParticipant(context.Background(), recipient, 0)
		if len(actual.Items) != len(recieverReceipts) {
			t.Fatalf("expected %v got %v", len(recieverReceipts), len(actual.Items))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, recieverReceipts[i][k]) {
					t.Fatalf("not today satan")
				}
			}
		}
	})
}
