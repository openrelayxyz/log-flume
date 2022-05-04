package indexer

import (
	// "encoding/binary"
	// "fmt"
	"log"
	// "io"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	// "github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/ethereum/go-ethereum/common"
	gtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	// ctypes "github.com/openrelayxyz/cardinal-types"
	// "golang.org/x/crypto/sha3"
	// "math/big"
	"regexp"
	// "sync"
	"strconv"
)

var (
	receiptRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/r/([0-9a-z]+)")
)

type txBundle struct {
	tx *gtypes.Transaction
	rm *cardinalReceiptMeta
	logs map[int]*gtypes.Log
}

type cardinalReceiptMeta struct {
	ContractAddress common.Address
	CumulativeGasUsed uint64
	GasUsed uint64
	LogsBloom []byte
	Status uint64
	LogCount uint
	LogOffset uint
}

type TxIndexer struct {
	chainid uint64
}


func (indexer *TxIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	receiptData := make(map[int]*cardinalReceiptMeta)
	txData := make(map[int]*gtypes.Transaction)

	for k, v := range pb.Values {
		switch {
		case txRegexp.MatchString(k):
			parts := txRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			tx := &gtypes.Transaction{}
			tx.UnmarshalBinary(v)
			txData[int(txIndex)] = tx
		case receiptRegexp.MatchString(k):
			parts := receiptRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			rmeta := &cardinalReceiptMeta{}
			rlp.DecodeBytes(v, rmeta)
			receiptData[int(txIndex)] = rmeta
		default:
		}
	}

	statements := make([]string, 0, len(txData) +1)

	statements = append(statements, applyParameters("DELETE FROM transactions WHERE block >= %v", pb.Number))

	for i := 0; i < len(txData); i++ {
		transaction := txData[int(i)]
		receipt := receiptData[int(i)]
		v, r, s := transaction.RawSignatureValues()

		//the following is all relative to calculating the sender, signer, "from"
		typ := transaction.Type()
		var signer gtypes.Signer
		senderMap := make(map[common.Hash]<-chan common.Address)
		ch := make(chan common.Address, 1)
		senderMap[transaction.Hash()] = ch
				switch {
				case typ == gtypes.AccessListTxType:
					signer = gtypes.NewEIP2930Signer(transaction.ChainId())
				case typ == gtypes.DynamicFeeTxType:
					signer = gtypes.NewLondonSigner(transaction.ChainId())
				// case uint64(pb.Number) > eip155Block: TODO: this is old logic, dummy logic below, we need to sort a solution for calculating or importing eip155 block number
				case uint64(pb.Number) > uint64(333):
					signer = gtypes.NewEIP155Signer(transaction.ChainId())
				// case uint64(pb.Number) > homesteadBlock: TODO: same WHERE
			case uint64(pb.Number) > uint64(222):
					signer = gtypes.HomesteadSigner{}
				default:
					signer = gtypes.FrontierSigner{}
				}
				sender, err := gtypes.Sender(signer, transaction)
				if err != nil {
					log.Printf("WARN: Failed to derive sender: %v", err.Error())
				}
				ch <- sender
			// }//(transaction, ch) //unclear on syntax here

		statements = append(statements, applyParameters(
			"INSERT INTO transactions(block, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, `value`, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, `status`, `type`, access_list, gasFeeCap, gasTipCap) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
			pb.Number,
			transaction.Gas(),
			transaction.GasPrice().Uint64(),
			transaction.Hash(),
			getCopy(compress(transaction.Data())),
			transaction.Nonce(),
			trimPrefix(transaction.To().Bytes()),
			int(i), //TODO: check type
			trimPrefix(transaction.Value().Bytes()),
			v.Int64(),
			r,
			s,
			sender,
			getFuncSig(transaction.Data()),
			nullZeroAddress(receipt.ContractAddress),
			receipt.CumulativeGasUsed,
			receipt.GasUsed,
			getCopy(compress(receipt.LogsBloom)),
			receipt.Status,
			transaction.Type(),
			transaction.AccessList(),
			trimPrefix(transaction.GasFeeCap().Bytes()),
			trimPrefix(transaction.GasTipCap().Bytes()),
			))
	}
	return statements, nil
}
