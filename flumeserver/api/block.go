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
  "database/sql"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/core/types"
  // "github.com/ethereum/go-ethereum/eth/filters"
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  "context"
  "fmt"
  "log"
  // "sync"
)

type BlockAPI struct {
	db *sql.DB
	network uint64
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

func NewBlockAPI (db *sql.DB, network uint64 ) *BlockAPI {
	return &BlockAPI{
		db: db,
		network: network,
	}
}


// func getLatestBlock(ctx context.Contex, db *sql.DB,) (int64, error) {
//   var result int64
//   var hash []byte
//   err := db.QueryRowContext(ctx, "SELECT max(number), hash FROM blocks;").Scan(&result, &hash)
//   return result, err
// }

func uintToHexBig(a uint64) *hexutil.Big {
  x := hexutil.Big(*new(big.Int).SetUint64(a))
  return &x
}

func bytesToHexBig(a []byte) *hexutil.Big {
  x := hexutil.Big(*new(big.Int).SetBytes(a))
  return &x
}


// case "eth_blockNumber":
// 	getBlockNumber(r.Context(), w, call, db, chainid)
// "eth_getBlockByNumber":
// 	getBlockByNumber(r.Context(), w, call, db, chainid)
// case "eth_getBlockByHash":
// 	getBlockByHash(r.Context(), w, call, db, chainid)
// case "eth_getBlockTransactionCountByNumber":
// 	getBlockTransactionCountByNumber(r.Context(), w, call, db, chainid)
// case "eth_getBlockTransactionCountByHash":
// 	getBlockTransactionCountByHash(r.Context(), w, call, db, chainid)
// case "eth_getUncleCountByBlockNumber":
// 	getUncleCountByBlockNumber(r.Context(), w, call, db, chainid)
// case "eth_getUncleCountByBlockHash":
// 	getUncleCountByBlockHash(r.Context(), w, call, db, chainid)

func (api *BlockAPI) Goodbuy() string {
	return "hello Clarice"
}

func getTransactionsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query string, params ...interface{}) ([]*rpcTransaction, error) {
  rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
  if err != nil { return nil, err }
  defer rows.Close()
  results := []*rpcTransaction{}
  for rows.Next() {
    var amount, to, from, data, blockHashBytes, txHash, r, s, cAccessListRLP, baseFeeBytes, gasFeeCapBytes, gasTipCapBytes []byte
    var nonce, gasLimit, blockNumber, gasPrice, txIndex, v uint64
    var txTypeRaw sql.NullInt32
    err := rows.Scan(
      &blockHashBytes,
      &blockNumber,
      &gasLimit,
      &gasPrice,
      &txHash,
      &data,
      &nonce,
      &to,
      &txIndex,
      &amount,
      &v,
      &r,
      &s,
      &from,
      &txTypeRaw,
      &cAccessListRLP,
      &baseFeeBytes,
      &gasFeeCapBytes,
      &gasTipCapBytes,
    )
    if err != nil { return nil, err }
    txType := uint8(txTypeRaw.Int32)
    blockHash := bytesToHash(blockHashBytes)
    txIndexHex := hexutil.Uint64(txIndex)
    inputBytes, err := decompress(data)
    if err != nil { return nil, err }
    accessListRLP, err := decompress(cAccessListRLP)
    if err != nil { return nil, err }
    var accessList *types.AccessList
    var chainID, gasFeeCap, gasTipCap *hexutil.Big
    switch txType {
    case types.AccessListTxType:
      accessList = &types.AccessList{}
      rlp.DecodeBytes(accessListRLP, accessList)
      chainID = uintToHexBig(chainid)
    case types.DynamicFeeTxType:
      accessList = &types.AccessList{}
      rlp.DecodeBytes(accessListRLP, accessList)
      chainID = uintToHexBig(chainid)
      gasFeeCap = bytesToHexBig(gasFeeCapBytes)
      gasTipCap = bytesToHexBig(gasTipCapBytes)
    case types.LegacyTxType:
      chainID = nil
    }
    results = append(results, &rpcTransaction{
      BlockHash: &blockHash,                  //*common.Hash
      BlockNumber: uintToHexBig(blockNumber), //*hexutil.Big
      From: bytesToAddress(from),             //common.Address
      Gas: hexutil.Uint64(gasLimit),          //hexutil.Uint64
      GasPrice:  uintToHexBig(gasPrice),      //*hexutil.Big
      GasFeeCap: gasFeeCap,                   //*hexutil.Big
      GasTipCap: gasTipCap,                   //*hexutil.Big
      Hash: bytesToHash(txHash),              //common.Hash
      Input: hexutil.Bytes(inputBytes),       //hexutil.Bytes
      Nonce: hexutil.Uint64(nonce),           //hexutil.Uint64
      To: bytesToAddressPtr(to),              //*common.Address
      TransactionIndex: &txIndexHex,          //*hexutil.Uint64
      Value: bytesToHexBig(amount),           //*hexutil.Big
      V: uintToHexBig(v),                     //*hexutil.Big
      R: bytesToHexBig(r),                    //*hexutil.Big
      S: bytesToHexBig(s),                    //*hexutil.Big
      Type: hexutil.Uint64(txType),
      ChainID: chainID,
      Accesses: accessList,
    })
  }
  if err := rows.Err(); err != nil { return nil, err }
  return results, nil
}

func getTransactionsBlock(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]*rpcTransaction, error) {
	query := fmt.Sprintf("SELECT blocks.hash, block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap FROM transactions INNER JOIN blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.transactionIndex LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
}

func getBlocks(ctx context.Context, db *sql.DB, includeTxs bool, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
  query := fmt.Sprintf("SELECT hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, extra, mixDigest, uncles, td, number, gasLimit, gasUsed, time, nonce, size, baseFee FROM blocks WHERE %v;", whereClause)
  rows, err := db.QueryContext(ctx, query, params...)
  if err != nil { return nil, err }
  defer rows.Close()
  results := []map[string]interface{}{}
  for rows.Next() {
    var hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloomBytes, extra, mixDigest, uncles, td, baseFee []byte
    var number, gasLimit, gasUsed, time, size, difficulty uint64
    var nonce int64
    err := rows.Scan(&hash, &parentHash, &uncleHash, &coinbase, &root, &txRoot, &receiptRoot, &bloomBytes, &difficulty, &extra, &mixDigest, &uncles, &td, &number, &gasLimit, &gasUsed, &time, &nonce, &size, &baseFee)
    if err != nil { return nil, err }
    logsBloom, err := decompress(bloomBytes)
    if err != nil {
      log.Printf("Error decompressing data: '%#x'", bloomBytes)
      return nil, err
    }
    unclesList := []common.Hash{}
    rlp.DecodeBytes(uncles, &unclesList)
    fields := map[string]interface{}{
      "difficulty": hexutil.Uint64(difficulty),
      "extraData": hexutil.Bytes(extra),
      "gasLimit": hexutil.Uint64(gasLimit),
      "gasUsed": hexutil.Uint64(gasUsed),
      "hash": bytesToHash(hash),
      "logsBloom": hexutil.Bytes(logsBloom),
      "miner": bytesToAddress(coinbase),
      "mixHash": bytesToHash(mixDigest),
      "nonce": types.EncodeNonce(uint64(nonce)),
      "number": hexutil.Uint64(number),
      "parentHash": bytesToHash(parentHash),
      "receiptsRoot": bytesToHash(receiptRoot),
      "sha3Uncles": bytesToHash(uncleHash),
      "size": hexutil.Uint64( size),
      "stateRoot": bytesToHash(root),
      "timestamp": hexutil.Uint64(time),
      "totalDifficulty": bytesToHexBig(td),
      "transactionsRoot": bytesToHash(txRoot),
      "uncles": unclesList,
    }
    if includeTxs {
      fields["transactions"], err = getTransactionsBlock(ctx, db, 0, 100000, chainid, "block = ?", number)
      if err != nil { return nil, err }
    } else {
      txs := []common.Hash{}
      txRows, err := db.QueryContext(ctx, "SELECT hash FROM transactions WHERE block = ? ORDER BY transactionIndex ASC", number)
      if err != nil { return nil, err }
      for txRows.Next() {
        var txHash []byte
        if err := txRows.Scan(&txHash); err != nil { return nil, err }
        txs = append(txs, bytesToHash(txHash))
      }
      if err := txRows.Err(); err != nil { return nil, err }
      fields["transactions"] = txs
    }
    if len(baseFee) > 0 {
      fields["baseFeePerGas"] = bytesToHexBig(baseFee)
    }
    results = append(results, fields)
  }
  if err := rows.Err(); err != nil {
    return nil, err
  }
  return results, nil
}

func txCount(ctx context.Context, db *sql.DB, whereClause string, params ...interface{}) (hexutil.Uint64, error) {
  var count uint64
  err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM transactions WHERE %v", whereClause), params...).Scan(&count)
  return hexutil.Uint64(count), err
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
	blockNumber = rpc.BlockNumber(bkNumber)
  count, err := txCount(ctx, api.db, "block = ?", blockNumber)
  if err != nil {return 0, err}
return count, nil
}

// func getBlockTransactionCountByHash(ctx context.Context, bkHash common.Hash) {
//   var blockHash common.Hash
//   if err := json.Unmarshal(call.Params[0], &blockHash); err != nil {
//     handleError(w, "error reading params.0", call.ID, 400)
//     return
//   }
//   var count uint64
//   if err := db.QueryRowContext(ctx, "SELECT count(*) FROM v_transactions WHERE blockHash = ?", trimPrefix(blockHash.Bytes())).Scan(&count); err != nil {
//     handleError(w, err.Error(), call.ID, 500)
//     return
//   }
//   responseBytes, err := json.Marshal(formatResponse(hexutil.Uint64(count), call))
//   if err != nil {
//     handleError(w, err.Error(), call.ID, 500)
//     return
//   }
//   w.WriteHeader(200)
//   w.Write(responseBytes)
// }
