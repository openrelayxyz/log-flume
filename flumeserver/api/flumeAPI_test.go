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

func getTransactionList(jsonBlockObject []map[string]json.RawMessage, address, key string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	transactions := getTransactionsForTesting(jsonBlockObject)
	addr, _ := json.Marshal(address)
	for _, tx := range transactions {
		if bytes.Equal(tx[key], addr) {
			results = append(results, tx)
		}
	}
	return results
}

func getParticipantTransactionList(jsonBlockObject []map[string]json.RawMessage, address, keyOne, keyTwo string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	transactions := getTransactionsForTesting(jsonBlockObject)
	addr, _ := json.Marshal(address)
	for _, tx := range transactions {
		if bytes.Equal(tx[keyOne], addr) || bytes.Equal(tx[keyTwo], addr) {
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

func getReceiptList(jsonReceiptObject []map[string]json.RawMessage, address, key string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	addr, _ := json.Marshal(address)
	for _, receipt := range jsonReceiptObject {
		if bytes.Equal(receipt[key], addr) {
			results = append(results, receipt)
		}
	}
	return results
}

func getParticipantReceiptList(jsonReceiptObject []map[string]json.RawMessage, address, keyOne, keyTwo string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	addr, _ := json.Marshal(address)
	for _, receipt := range jsonReceiptObject {
		if bytes.Equal(receipt[keyOne], addr) || bytes.Equal(receipt[keyTwo], addr) {
			results = append(results, receipt)
		}
	}
	return results
}

var (
	senderAddr    = "0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5"
	recipientAddr = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
	genericAddr   = "0x3cd751e6b0078be393132286c442345e5dc49699"
)

func TestFlumeAPI(t *testing.T) {
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
						t.Fatalf("getTransactionReceiptsByBlockHash test failed on hash %v, index %v, key %v", hash, j, k)
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
						t.Fatalf("getTransactionReceiptsByBlockNumber test failed on block %v, index %v, key %v", number, j, k)
					}
				}
			}
		})
	}
	senderTxns := getTransactionList(blockObject, senderAddr, "from")
	sender := common.HexToAddress(senderAddr)
	if len(senderTxns) != 47 {
		t.Fatalf("sender transactions list of incorrect length expected 47 got %v", len(senderTxns))
	}
	t.Run(fmt.Sprintf("GetTransactionsBySender"), func(t *testing.T) {
		actual, _ := f.GetTransactionsBySender(context.Background(), sender, nil)
		if len(actual.Items) != len(senderTxns) {
			t.Fatalf("getTransactionsBySender result of incorrect length expected %v got %v", len(actual.Items), len(senderTxns))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, senderTxns[i][k]) {
					t.Fatalf("getTransactionsBySender error index %v, key %v", i, k)
				}
			}
		}
	})
	senderReceipts := getReceiptList(receiptObject, senderAddr, "from")
	if len(senderReceipts) != 47 {
		t.Fatalf("sender transactions list of incorrect length expected 47 got %v", len(senderReceipts))
	}
	t.Run(fmt.Sprintf("GetTransactionReceiptsBySender"), func(t *testing.T) {
		actual, _ := f.GetTransactionReceiptsBySender(context.Background(), sender, nil)
		if len(actual.Items) != len(senderReceipts) {
			t.Fatalf("getTransactionReceiptsBySender result of incorrect length expected %v got %v", len(actual.Items), len(senderReceipts))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, senderReceipts[i][k]) {
					t.Fatalf("getTransactionReceiptsBySender error index %v, key %v", i, k)
				}
			}
		}
	})
	recipientTxns := getTransactionList(blockObject, recipientAddr, "to")
	recipient := common.HexToAddress(recipientAddr)
	if len(recipientTxns) != 141 {
		t.Fatalf("recipient transactions list of incorrect length expected 107 got %v", len(recipientTxns))
	}
	t.Run(fmt.Sprintf("GetTransactionsByRecipient"), func(t *testing.T) {
		actual, _ := f.GetTransactionsByRecipient(context.Background(), recipient, nil)
		if len(actual.Items) != len(recipientTxns) {
			t.Fatalf("getTransactionsByRecipient result of incorrect length expected %v got %v", len(actual.Items), len(recipientTxns))
		}
		for i, tx := range actual.Items {

			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, recipientTxns[i][k]) {
					t.Fatalf("getTransactionsByRecipient error index %v, key %v", i, k)
				}
			}
		}
	})
	recipientReceipts := getReceiptList(receiptObject, recipientAddr, "to")
	if len(recipientReceipts) != 141 {
		t.Fatalf("recipient transactions list of incorrect length expected 107 got %v", len(recipientReceipts))
	}
	t.Run(fmt.Sprintf("GetTransactionsReceiptsByRecipient"), func(t *testing.T) {
		actual, _ := f.GetTransactionReceiptsByRecipient(context.Background(), recipient, nil)
		if len(actual.Items) != len(recipientReceipts) {
			t.Fatalf("getTransactionReceiptsByRecipient result of incorrect length expected %v got %v", len(actual.Items), len(recipientReceipts))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, recipientReceipts[i][k]) {
					t.Fatalf("getTransactionReceiptsByRecipient error index %v, key %v", i, k)
				}
			}
		}
	})
	participantTxns := getParticipantTransactionList(blockObject, genericAddr, "to", "from")
	participant := common.HexToAddress(genericAddr)
	t.Run(fmt.Sprintf("GetTransactionsByParicipant"), func(t *testing.T) {
		actual, _ := f.GetTransactionsByParticipant(context.Background(), participant, nil)
		if len(actual.Items) != len(participantTxns) {
			t.Fatalf("getTransactionsByParticipant result of incorrect length expected %v got %v", len(actual.Items), len(participantTxns))
		}
		for i, tx := range actual.Items {

			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, participantTxns[i][k]) {
					t.Fatalf("getTransactionsByParticipant error index %v, key %v", i, k)
				}
			}
		}
	})
	participantReceipts := getParticipantReceiptList(receiptObject, genericAddr, "to", "from")
	t.Run(fmt.Sprintf("GetTransactionsReceiptsByParticipant"), func(t *testing.T) {
		actual, _ := f.GetTransactionReceiptsByParticipant(context.Background(), participant, nil)
		if len(actual.Items) != len(participantReceipts) {
			t.Fatalf("getTransactionReceiptsByParticipant result of incorrect length expected %v got %v", len(actual.Items), len(participantReceipts))
		}
		for i, tx := range actual.Items {
			for k, v := range tx {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, participantReceipts[i][k]) {
					t.Fatalf("getTransactionReceiptsByParticipant (recipient) error index %v, key %v", i, k)
				}
			}
		}
	})
}
