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
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  "context"
  // "fmt"
  // "log"
  // "sync"
)

type BlockAPI struct {
	db *sql.DB
	network uint64
}

func NewBlockAPI (db *sql.DB, network uint64 ) *BlockAPI {
	return &BlockAPI{
		db: db,
		network: network,
	}
}

// case "eth_blockNumber": CHECK
// 	getBlockNumber(r.Context(), w, call, db, chainid)
// "eth_getBlockByNumber": CHECK
// 	getBlockByNumber(r.Context(), w, call, db, chainid)
// case "eth_getBlockByHash": CHECK
// 	getBlockByHash(r.Context(), w, call, db, chainid)
// case "eth_getBlockTransactionCountByNumber": CHECK
// 	getBlockTransactionCountByNumber(r.Context(), w, call, db, chainid)
// case "eth_getBlockTransactionCountByHash": CHECK
// 	getBlockTransactionCountByHash(r.Context(), w, call, db, chainid)
// case "eth_getUncleCountByBlockNumber":
// 	getUncleCountByBlockNumber(r.Context(), w, call, db, chainid)
// case "eth_getUncleCountByBlockHash":
// 	getUncleCountByBlockHash(r.Context(), w, call, db, chainid)

func (api *BlockAPI) Block() string {
	return "hello Clarice"
}

	func (api *BlockAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	  blockNo, err := getLatestBlock(ctx, api.db)
		if err != nil {return 0, err}
	  return hexutil.Uint64(blockNo), nil
	}

	func (api *BlockAPI) GetBlockByNumber(ctx context.Context, bkNumber rpc.BlockNumber, txns bool) (interface{}, error) {
		var err error
		var includeTxs bool
	  var blockNumber rpc.BlockNumber
		includeTxs = txns
		blockNumber = rpc.BlockNumber(bkNumber)
	  blocks, err := getBlocks(ctx, api.db, includeTxs, api.network, "number = ?", blockNumber)
	  if err != nil {
	    return nil, err
	  }
	  var blockVal interface{}
	  if len(blocks) > 0 {
	    blockVal = blocks[0]
	  }
	return blockVal, nil
	}

	func (api *BlockAPI) GetBlockByHash(ctx context.Context, bkHash common.Hash, txns bool) (interface{}, error) {
		var err error
		var includeTxs bool
		var blockHash common.Hash
		includeTxs = txns
		blockHash = bkHash
	  blocks, err := getBlocks(ctx, api.db, includeTxs, api.network, "hash = ?", trimPrefix(blockHash.Bytes()))
		if err != nil {return nil, err}
	  var blockVal interface{}
	  if len(blocks) > 0 {
	    blockVal = blocks[0]
	  }
	return blockVal, nil
}

func (api *BlockAPI) GetBlockTransactionCountByNumber(ctx context.Context, bkNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	var err error
  var blockNumber rpc.BlockNumber
	var count hexutil.Uint64

	blockNumber = rpc.BlockNumber(bkNumber)
  count, err = txCount(ctx, api.db, "block = ?", blockNumber)
  if err != nil {return 0, err}
return count, nil
}

func (api *BlockAPI) GetBlockTransactionCountByHash(ctx context.Context, bkHash common.Hash) (hexutil.Uint64, error) {
	var err error
  var blockHash common.Hash
  var count hexutil.Uint64
	blockHash = bkHash

  if err = api.db.QueryRowContext(ctx, "SELECT count(*) FROM v_transactions WHERE blockHash = ?", trimPrefix(blockHash.Bytes())).Scan(&count); err != nil {
    return 0, err
  }
	return count, nil
}

func (api *BlockAPI) GetUncleCountByBlockNumber(ctx context.Context, bkNumber rpc.BlockNumber) ([]common.Hash, error) {
	var blockNumber rpc.BlockNumber
	var uncles []byte
	unclesList := []common.Hash{}

	blockNumber = rpc.BlockNumber(bkNumber)
  if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE number = ?", blockNumber).Scan(&uncles); err != nil {
    return nil, err
  }
  rlp.DecodeBytes(uncles, &unclesList)

	return unclesList, nil
}

func (api *BlockAPI) GetUncleCountByBlockHash(ctx context.Context, bkHash common.Hash) ([]common.Hash, error) {
	var err error
	var blockHash common.Hash
	var uncles []byte
	unclesList := []common.Hash{}

	blockHash = bkHash
  if err = api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE hash = ?", trimPrefix(blockHash.Bytes())).Scan(&uncles); err != nil {
    return nil, err
  }
  rlp.DecodeBytes(uncles, &unclesList)

	return unclesList, nil
}
