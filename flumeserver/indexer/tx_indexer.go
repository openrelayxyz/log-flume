package indexer

import (
	"log"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/ethereum/go-ethereum/common"
	gtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"regexp"
	"strconv"
)

var (
	receiptRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/r/([0-9a-z]+)")
)

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
	eip155Block uint64
	homesteadBlock uint64
}

func NewTxIndexer(eip155block, homesteadblock uint64) Indexer {
	return &TxIndexer{
		eip155Block: eip155block,
		homesteadBlock: homesteadblock,
	}
}

func (indexer *TxIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	receiptData := make(map[int]*cardinalReceiptMeta)
	txData := make(map[int]*gtypes.Transaction)
	senderMap := make(map[common.Hash]<-chan common.Address)

	for k, v := range pb.Values {
		switch {
		case txRegexp.MatchString(k):
			parts := txRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			tx := &gtypes.Transaction{}
			tx.UnmarshalBinary(v)

			var signer gtypes.Signer
			ch := make(chan common.Address, 1)
			senderMap[tx.Hash()] = ch
			go func(transaction *gtypes.Transaction, ch chan<- common.Address) {
				switch {
				case transaction.Type() == gtypes.AccessListTxType:
					signer = gtypes.NewEIP2930Signer(transaction.ChainId())
				case transaction.Type() == gtypes.DynamicFeeTxType:
					signer = gtypes.NewLondonSigner(transaction.ChainId())
				case uint64(pb.Number) > indexer.eip155Block:
					signer = gtypes.NewEIP155Signer(transaction.ChainId())
				case uint64(pb.Number) > indexer.homesteadBlock:
					signer = gtypes.HomesteadSigner{}
				default:
					signer = gtypes.FrontierSigner{}
				}
				sender, err := gtypes.Sender(signer, tx)
				if err != nil {
					log.Panic(err.Error())
				}
				ch <- sender
			}(tx, ch)

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
		sender := <-senderMap[transaction.Hash()]
		//do we need further error handling here a la indexer 313?
		v, r, s := transaction.RawSignatureValues()

		statements = append(statements, applyParameters(
			"INSERT INTO transactions(block, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, `value`, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, `status`, `type`, access_list, gasFeeCap, gasTipCap) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
			pb.Number,
			transaction.Gas(),
			transaction.GasPrice().Uint64(),
			transaction.Hash(),
			getCopy(compress(transaction.Data())),
			transaction.Nonce(),
			trimPrefix(transaction.To().Bytes()),
			uint(i),
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
