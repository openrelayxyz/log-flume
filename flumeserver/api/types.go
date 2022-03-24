package api

import (
  // "sort"
  // "bytes"
  // "github.com/klauspost/compress/zlib"
  // "strings"
  // "time"
  // "encoding/json"
  "math/big"
  // "net/http"
  // "database/sql"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/core/types"
  // "github.com/ethereum/go-ethereum/eth/filters"
  // "github.com/ethereum/go-ethereum/rlp"
  // "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  // "context"
  // "fmt"
  // "log"
  // "sync"
)

type bigList []*big.Int

func (ms bigList) Len() int {
	return len(ms)
}

func (ms bigList) Less(i, j int) bool {
	return ms[i].Cmp(ms[j]) < 0
}

func (ms bigList) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

type sortLogs []*types.Log


func (ms sortLogs) Len() int {
  return len(ms)
}

func (ms sortLogs) Less(i, j int) bool {
  if ms[i].BlockNumber != ms[j].BlockNumber {
    return ms[i].BlockNumber < ms[j].BlockNumber
  }
  return ms[i].Index < ms[j].Index
}

func (ms sortLogs) Swap(i, j int) {
  ms[i], ms[j] = ms[j], ms[i]
}

type feeHistoryResult struct {
	OldestBlock  *hexutil.Big     `json:"oldestBlock"`
	Reward       [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee      []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio []float64        `json:"gasUsedRatio"`
}

// txGasAndReward is sorted in ascending order based on reward
type (
	txGasAndReward struct {
		gasUsed uint64
		reward  *big.Int
	}
	sortGasAndReward []txGasAndReward
)

func (s sortGasAndReward) Len() int { return len(s) }
func (s sortGasAndReward) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s sortGasAndReward) Less(i, j int) bool {
	return s[i].reward.Cmp(s[j].reward) < 0
}

type paginator struct {
  Items interface{} `json:"items"`
  Token interface{} `json:"next,omitempty"`
}

type Block struct {
	BaseFeePerGas *hexutil.Big`json:"baseFeePerGas,omitempty"`
	Difficulty hexutil.Uint64 `json:"difficulty"`
	ExtraData hexutil.Bytes `json:"extraData"`
	GasLimit hexutil.Uint64 `json:"gasLimit"`
	GasUsed hexutil.Uint64 `json:"gasUsed"`
	Hash common.Hash `json:"hash"`
	LogsBloom hexutil.Bytes `json:"logsBloom"`
	Miner common.Address `json:"miner"`
	MixHash common.Hash `json:"mixHash"`
	Nonce types.BlockNonce `json:"nonce"`
	Number hexutil.Uint64 `json:"number"`
	ParentHash common.Hash `json:"parentHash"`
	ReceiptsRoot common.Hash `json:"receiptsRoot"`
	Sha3Uncles common.Hash `json:"sha3Uncles"`
	Size hexutil.Uint64 `json:"size"`
	StateRoot common.Hash `json:"stateRoot"`
	Timestamp hexutil.Uint64 `json:"timeStamp"`
	TotalDifficulty *hexutil.Big `json:"totalDifficulty"`
	Transactions interface{} `json:"transactiions"`
	TransactionsRoot common.Hash `json:"transactionsRoot"`
	Uncles []common.Hash `json:"uncles"`
}

type TransactionTypeOne struct {
	Transactions []*rpcTransaction `json:"transactions"`
}

type TransactionTypeTwo struct {
	Transactions []common.Hash `json:"transactions"`
}


type rpcTransaction struct {
  BlockHash        *common.Hash      `json:"blockHash"`
  BlockNumber      *hexutil.Big      `json:"blockNumber"`
  From             common.Address    `json:"from"`
  Gas              hexutil.Uint64    `json:"gas"`
  GasPrice         *hexutil.Big      `json:"gasPrice"`
  GasFeeCap        *hexutil.Big      `json:"maxFeePerGas,omitempty"`
	GasTipCap        *hexutil.Big      `json:"maxPriorityFeePerGas,omitempty"`
  Hash             common.Hash       `json:"hash"`
  Input            hexutil.Bytes     `json:"input"`
  Nonce            hexutil.Uint64    `json:"nonce"`
  To               *common.Address   `json:"to"`
  TransactionIndex *hexutil.Uint64   `json:"transactionIndex"`
  Value            *hexutil.Big      `json:"value"`
  Type             hexutil.Uint64    `json:"type"`
  Accesses         *types.AccessList `json:"accessList,omitempty"`
  ChainID          *hexutil.Big      `json:"chainId,omitempty"`
  V                *hexutil.Big      `json:"v"`
  R                *hexutil.Big      `json:"r"`
  S                *hexutil.Big      `json:"s"`
}
