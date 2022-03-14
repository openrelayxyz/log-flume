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
  // "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  "context"
  // "fmt"
  "log"
  // "sync"
)

type FlumeAPI struct {
	db *sql.DB
	network uint64
}

func NewFlumeAPI (db *sql.DB, network uint64 ) *FlumeAPI {
	return &FlumeAPI{
		db: db,
		network: network,
	}
}

func (api *FlumeAPI) Flume() string {
	return "goodbuy horses"
}

func (api *FlumeAPI) GetTransactionsBySender(ctx context.Context, address common.Address, offset int) (interface{}, error) {

  txs, err := getPendingTransactions(ctx, api.db, offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting pending txs: %v", err.Error())
    return nil, err
  }
  ctxs, err := getTransactions(ctx, api.db, offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting txs: %v", err.Error())
    return nil, err
  }
  txs = append(txs, ctxs...)
  result := paginator{Items: txs}
  if len(txs) >= 1000 {
    result.Token = offset + len(txs)
  }
  return result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsBySender(ctx context.Context, address common.Address, offset int) (interface{}, error) {

  receipts, err := getTransactionReceipts(ctx, api.db, offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    return nil, err
  }
  result := paginator{Items: receipts}
  if len(receipts) == 1000 {
    result.Token = offset + len(receipts)
  }

	return result, nil
}

func (api *FlumeAPI) GetTransactionsByRecipient(ctx context.Context, address common.Address, offset int) (interface{}, error) {

  txs, err := getPendingTransactions(ctx, api.db, offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting pending txs: %v", err.Error())
    return nil, err
  }
  ctxs, err := getTransactions(ctx, api.db, offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting txs: %v", err.Error())
    return nil, err
  }
  txs = append(txs, ctxs...)
  result := paginator{Items: txs}
  if len(txs) >= 1000 {
    result.Token = offset + len(txs)
  }
  return result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByRecipient(ctx context.Context, address common.Address, offset int) (interface{}, error) {

  receipts, err := getTransactionReceipts(ctx, api.db, offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    return nil, err
  }
  result := paginator{Items: receipts}
  if len(receipts) == 1000 {
    result.Token = offset + len(receipts)
  }
	return result, nil
}

func (api *FlumeAPI) GetTransactionsByParticipant(ctx context.Context, address common.Address, offset int) (interface{}, error) {

  txs, err := getPendingTransactions(ctx, api.db, offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting pending txs: %v", err.Error())
    return nil, err
  }
  ctxs, err := getTransactions(ctx, api.db, offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting txs: %v", err.Error())
    return nil, err
  }
  txs = append(txs, ctxs...)
  result := paginator{Items: txs}
  if len(txs) >= 1000 {
    result.Token = offset + len(txs)
  }

	return result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByParticipant(ctx context.Context, address common.Address, offset int) (interface{}, error){

  receipts, err := getTransactionReceipts(ctx, api.db, offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    return nil, err
  }
  result := paginator{Items: receipts}
  if len(receipts) == 1000 {
    result.Token = offset + len(receipts)
  }

	return result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByBlockHash(ctx context.Context, blockHash common.Hash) ([]map[string]interface{}, error) {

  // Offset and limit aren't too relevant here, as there's a limit on
  // transactions per block.
  receipts, err := getTransactionReceiptsBlock(ctx, api.db, 0, 100000, api.network, "blocks.hash = ?", trimPrefix(blockHash.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    return nil, err
  }
  return receipts, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByBlockNumber(ctx context.Context, blockNumber hexutil.Uint64) ([]map[string]interface{}, error) {

  receipts, err := getTransactionReceiptsBlock(ctx, api.db, 0, 100000, api.network, "block = ?", uint64(blockNumber))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    return nil, err
  }
	return receipts, nil
}
