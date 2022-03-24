package api

import (
	// "sort"
  // "bytes"
  // "github.com/klauspost/compress/zlib"
  // "strings"
  // "time"
  // "encoding/json"
  // "math/big"
  // "net/http"
  "database/sql"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  // "github.com/ethereum/go-ethereum/core/types"
  // "github.com/ethereum/go-ethereum/eth/filters"
  // "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  "context"
  // "fmt"
  // "log"
  // "sync"
)

type TransactionAPI struct {
	db *sql.DB
	network uint64
}

func NewTransactionAPI (db *sql.DB, network uint64 ) *TransactionAPI {
	return &TransactionAPI{
		db: db,
		network: network,
	}
}


func (api *TransactionAPI) Transaction() string {
	return "goodbuy horses"
}

func (api *TransactionAPI) GetTransactionByHash(ctx context.Context, txHash common.Hash) (interface{}, error) {
	var err error
  txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
  if err != nil {
    return nil, err
  }
	if len(txs) == 0 {
		txs, err = getPendingTransactions(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	}
	if err != nil {
    return nil, err
  }
  result := returnSingleTransaction(txs)

	return result, nil
}

func (api *TransactionAPI) GetTransactionByBlockHashAndIndex(ctx context.Context, txHash common.Hash, index hexutil.Uint64) (interface{}, error) {
	var err error

  txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "blocks.hash = ? AND transactionIndex = ?", trimPrefix(txHash.Bytes()), uint64(index))
  if err != nil {
    return nil, err
  }
  returnSingleTransaction(txs)

	return txs, nil
}

func (api *TransactionAPI) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index hexutil.Uint64) (*rpcTransaction, error) {

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}

  txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "block = ? AND transactionIndex = ?", uint64(blockNumber), uint64(index))
  if err != nil {
    return nil, err
  }
  tx :=returnSingleTransaction(txs)

	return tx, nil
}

func (api *TransactionAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (interface{}, error) {
	var err error

  receipts, err := getTransactionReceiptsBlock(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
  if err != nil {
    return nil, err
  }
  returnSingleReceipt(receipts)

	return receipts, nil
}

func (api *TransactionAPI) GetTransactionCount(ctx context.Context, addr common.Address) (hexutil.Uint64, error){
	nonce, err := getSenderNonce(ctx, api.db, addr)
	if err != nil {
		return 0, err
	}

	return nonce, nil
}
