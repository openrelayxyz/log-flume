package api

import (
	"sort"
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
  "github.com/ethereum/go-ethereum/core/types"
  // "github.com/ethereum/go-ethereum/eth/filters"
  "github.com/ethereum/go-ethereum/rlp"
  // "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  "context"
  "fmt"
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


func (api *TransactionAPI) Transactions() string {
	return "goodbuy horses"
}

// case "eth_getTransactionByHash":
// 	getTransactionByHash(r.Context(), w, call, db, chainid)
// case "eth_getTransactionByBlockHashAndIndex":
// 	getTransactionByBlockHashAndIndex(r.Context(), w, call, db, chainid)
// case "eth_getTransactionByBlockNumberAndIndex":
// 	getTransactionByBlockNumberAndIndex(r.Context(), w, call, db, chainid)
// case "eth_getTransactionReceipt":
// 	getTransactionReceipt(r.Context(), w, call, db, chainid)
// case "eth_getBlockTransactionCountByNumber":
// 	getBlockTransactionCountByNumber(r.Context(), w, call, db, chainid)
// case "eth_getBlockTransactionCountByHash":
// 	getBlockTransactionCountByHash(r.Context(), w, call, db, chainid)
// case "eth_getTransactionCount":
// 	getTransactionCount(r.Context(), w, call, db, chainid)

func (api *TransactionAPI) getTransactionsBlock(ctx context.Context, offset, limit int, whereClause string, params ...interface{}) ([]*rpcTransaction, error) {
	query := fmt.Sprintf("SELECT blocks.hash, block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap FROM transactions INNER JOIN blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.transactionIndex LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, api.db, offset, limit, api.network, query, params...)
}

func (api *TransactionAPI) getTransactionsQuery(ctx context.Context, offset, limit int, query string, params ...interface{}) ([]*rpcTransaction, error) {
  rows, err := api.db.QueryContext(ctx, query, append(params, limit, offset)...)
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
      chainID = uintToHexBig(api.network)
    case types.DynamicFeeTxType:
      accessList = &types.AccessList{}
      rlp.DecodeBytes(accessListRLP, accessList)
      chainID = uintToHexBig(api.network)
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

func (api *TransactionAPI) returnSingleTransaction(txs []*rpcTransaction) interface{} {
  var result interface{}
  if len(txs) > 0 {
    result = txs[0]
  } else {
    result = nil
  }
return result
}

func (api *TransactionAPI) getPendingTransactions(ctx context.Context, offset, limit int, whereClause string, params ...interface{}) ([]*rpcTransaction, error) {
  query := fmt.Sprintf("SELECT transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, transactions.gasFeeCap, transactions.gasTipCap FROM mempool.transactions WHERE %v LIMIT ? OFFSET ?;", whereClause)
  rows, err := api.db.QueryContext(ctx, query, append(params, limit, offset)...)
  if err != nil { return nil, err }
  defer rows.Close()
  results := []*rpcTransaction{}
  for rows.Next() {
    var amount, to, from, data, txHash, r, s, cAccessListRLP, gasFeeCapBytes, gasTipCapBytes []byte
    var nonce, gasLimit, gasPrice, v uint64
    var txTypeRaw sql.NullInt32
    err := rows.Scan(
      &gasLimit,
      &gasPrice,
      &txHash,
      &data,
      &nonce,
      &to,
      &amount,
      &v,
      &r,
      &s,
      &from,
      &txTypeRaw,
      &cAccessListRLP,
      &gasFeeCapBytes,
      &gasTipCapBytes,
    )
    if err != nil { return nil, err }
    txType := uint8(txTypeRaw.Int32)
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
      chainID = uintToHexBig(api.network)
    case types.DynamicFeeTxType:
      accessList = &types.AccessList{}
      rlp.DecodeBytes(accessListRLP, accessList)
      chainID = uintToHexBig(api.network)
      gasFeeCap = bytesToHexBig(gasFeeCapBytes)
      gasTipCap = bytesToHexBig(gasTipCapBytes)
    case types.LegacyTxType:
      chainID = nil
    }
    results = append(results, &rpcTransaction{
      From: bytesToAddress(from),             //common.Address
      Gas: hexutil.Uint64(gasLimit),          //hexutil.Uint64
      GasPrice:  uintToHexBig(gasPrice),      //*hexutil.Big
      GasFeeCap: gasFeeCap,                   //*hexutil.Big
      GasTipCap: gasTipCap,                   //*hexutil.Big
      Hash: bytesToHash(txHash),              //common.Hash
      Input: hexutil.Bytes(inputBytes),       //hexutil.Bytes
      Nonce: hexutil.Uint64(nonce),           //hexutil.Uint64
      To: bytesToAddressPtr(to),              //*common.Address
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

func (api *TransactionAPI) getTransactionReceiptsQuery(ctx context.Context, offset, limit int, query, logsQuery string, params ...interface{}) ([]map[string]interface{}, error) {
  logRows, err := api.db.QueryContext(ctx, logsQuery, params...)
  if err != nil {
    return nil, err
  }
  txLogs := make(map[common.Hash]sortLogs)
  for logRows.Next() {
    var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
    var logIndex uint
		var blockNumber uint64
    err := logRows.Scan(&txHashBytes, &blockNumber, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
    if err != nil {
      logRows.Close()
      return nil, err
    }
    txHash := bytesToHash(txHashBytes)
    if _, ok := txLogs[txHash]; !ok {
      txLogs[txHash] = sortLogs{}
    }
    topics := []common.Hash{}
    if len(topic0) > 0 { topics = append(topics, bytesToHash(topic0)) }
    if len(topic1) > 0 { topics = append(topics, bytesToHash(topic1)) }
    if len(topic2) > 0 { topics = append(topics, bytesToHash(topic2)) }
    if len(topic3) > 0 { topics = append(topics, bytesToHash(topic3)) }
    input, err := decompress(data)
    if err != nil { return nil, err }
    txLogs[txHash] = append(txLogs[txHash], &types.Log{
      Address: bytesToAddress(address),
      Topics: topics,
      Data: input,
      BlockNumber: blockNumber,
      TxHash: txHash,
      Index: logIndex,
    })
  }
  logRows.Close()
  if err := logRows.Err(); err != nil {
    return nil, err
  }

  rows, err := api.db.QueryContext(ctx, query, append(params, limit, offset)...)
  if err != nil { return nil, err }
  defer rows.Close()
  results := []map[string]interface{}{}
  for rows.Next() {
    var to, from, blockHash, txHash, contractAddress, bloomBytes []byte
    var blockNumber, txIndex, gasUsed, cumulativeGasUsed, status, gasPrice  uint64
    var txTypeRaw sql.NullInt32
    err := rows.Scan(
      &blockHash,
      &blockNumber,
      &gasUsed,
      &cumulativeGasUsed,
      &txHash,
      &to,
      &txIndex,
      &from,
      &contractAddress,
      &bloomBytes,
      &status,
      &txTypeRaw,
      &gasPrice,
    )
    if err != nil { return nil, err }
    txType := uint8(txTypeRaw.Int32)
    logsBloom, err := decompress(bloomBytes)
    if err != nil { return nil, err }
    fields := map[string]interface{}{
      "blockHash":         bytesToHash(blockHash),
      "blockNumber":       hexutil.Uint64(blockNumber),
      "transactionHash":   bytesToHash(txHash),
      "transactionIndex":  hexutil.Uint64(txIndex),
      "from":              bytesToAddress(from),
      "to":                bytesToAddressPtr(to),
      "gasUsed":           hexutil.Uint64(gasUsed),
      "cumulativeGasUsed": hexutil.Uint64(cumulativeGasUsed),
      "effectiveGasPrice": hexutil.Uint64(gasPrice),
      "contractAddress":   nil,
      "logsBloom":         hexutil.Bytes(logsBloom),
      "status":            hexutil.Uint(status),
      "type":              hexutil.Uint(txType),
    }
    // If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
    if address := bytesToAddress(contractAddress); address != (common.Address{}) {
      fields["contractAddress"] = address
    }
    txh := bytesToHash(txHash)
    for i := range txLogs[txh] {
      txLogs[txh][i].TxIndex = uint(txIndex)
      txLogs[txh][i].BlockHash = bytesToHash(blockHash)
    }
    logs, ok := txLogs[txh]
		if !ok {
			logs = sortLogs{}
		}
		sort.Sort(logs)
		fields["logs"] = logs
    results = append(results, fields)
  }
  if err := rows.Err(); err != nil { return nil, err }
  return results, nil
}

func (api *TransactionAPI) getTransactionReceiptsBlock(ctx context.Context, offset, limit int, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, block, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice FROM transactions INNER JOIN blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.rowid LIMIT ? OFFSET ?;", whereClause)
	logsQuery := fmt.Sprintf(`
		SELECT transactionHash, block, address, topic0, topic1, topic2, topic3, data, logIndex
		FROM event_logs
		WHERE (transactionHash, block) IN (
			SELECT transactions.hash, block
			FROM transactions INNER JOIN blocks ON transactions.block = blocks.number
			WHERE %v
		);`, whereClause)
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
		func getPendingTransactions(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]*rpcTransaction, error) {
		  query := fmt.Sprintf("SELECT transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, transactions.gasFeeCap, transactions.gasTipCap FROM mempool.transactions WHERE %v LIMIT ? OFFSET ?;", whereClause)
		  rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
		  if err != nil { return nil, err }
		  defer rows.Close()
		  results := []*rpcTransaction{}
		  for rows.Next() {
		    var amount, to, from, data, txHash, r, s, cAccessListRLP, gasFeeCapBytes, gasTipCapBytes []byte
		    var nonce, gasLimit, gasPrice, v uint64
		    var txTypeRaw sql.NullInt32
		    err := rows.Scan(
		      &gasLimit,
		      &gasPrice,
		      &txHash,
		      &data,
		      &nonce,
		      &to,
		      &amount,
		      &v,
		      &r,
		      &s,
		      &from,
		      &txTypeRaw,
		      &cAccessListRLP,
		      &gasFeeCapBytes,
		      &gasTipCapBytes,
		    )
		    if err != nil { return nil, err }
		    txType := uint8(txTypeRaw.Int32)
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
		      From: bytesToAddress(from),             //common.Address
		      Gas: hexutil.Uint64(gasLimit),          //hexutil.Uint64
		      GasPrice:  uintToHexBig(gasPrice),      //*hexutil.Big
		      GasFeeCap: gasFeeCap,                   //*hexutil.Big
		      GasTipCap: gasTipCap,                   //*hexutil.Big
		      Hash: bytesToHash(txHash),              //common.Hash
		      Input: hexutil.Bytes(inputBytes),       //hexutil.Bytes
		      Nonce: hexutil.Uint64(nonce),           //hexutil.Uint64
		      To: bytesToAddressPtr(to),              //*common.Address
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
		func getTransactionReceipts(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
			query := fmt.Sprintf("SELECT blocks.hash, block, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice FROM transactions INNER JOIN blocks ON blocks.number = transactions.block WHERE transactions.rowid IN (SELECT transactions.rowid FROM transactions INNER JOIN blocks ON transactions.block = blocks.number WHERE %v) LIMIT ? OFFSET ?;", whereClause)
			logsQuery := fmt.Sprintf(`
				SELECT transactionHash, block, address, topic0, topic1, topic2, topic3, data, logIndex
				FROM event_logs
				WHERE (transactionHash, block) IN (
					SELECT transactions.hash, block
					FROM transactions INNER JOIN blocks ON transactions.block = blocks.number
					WHERE %v
				);`, whereClause)
			return getTransactionReceiptsQuery(ctx, db, offset, limit, chainid, query, logsQuery, params...)
		}
		func getTransactionReceiptsBlock(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
			query := fmt.Sprintf("SELECT blocks.hash, block, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice FROM transactions INNER JOIN blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.rowid LIMIT ? OFFSET ?;", whereClause)
			logsQuery := fmt.Sprintf(`
				SELECT transactionHash, block, address, topic0, topic1, topic2, topic3, data, logIndex
				FROM event_logs
				WHERE (transactionHash, block) IN (
					SELECT transactions.hash, block
					FROM transactions INNER JOIN blocks ON transactions.block = blocks.number
					WHERE %v
				);`, whereClause)
			return getTransactionReceiptsQuery(ctx, db, offset, limit, chainid, query, logsQuery, params...)
		}
		func getTransactionReceiptsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query, logsQuery string, params ...interface{}) ([]map[string]interface{}, error) {
		  logRows, err := db.QueryContext(ctx, logsQuery, params...)
		  if err != nil {
		    log.Printf("Error selecting logs : %v - '%v'", err.Error(), query)
		    return nil, err
		  }
		  txLogs := make(map[common.Hash]sortLogs)
		  for logRows.Next() {
		    var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
		    var logIndex uint
				var blockNumber uint64
		    err := logRows.Scan(&txHashBytes, &blockNumber, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
		    if err != nil {
		      logRows.Close()
		      return nil, err
		    }
		    txHash := bytesToHash(txHashBytes)
		    if _, ok := txLogs[txHash]; !ok {
		      txLogs[txHash] = sortLogs{}
		    }
		    topics := []common.Hash{}
		    if len(topic0) > 0 { topics = append(topics, bytesToHash(topic0)) }
		    if len(topic1) > 0 { topics = append(topics, bytesToHash(topic1)) }
		    if len(topic2) > 0 { topics = append(topics, bytesToHash(topic2)) }
		    if len(topic3) > 0 { topics = append(topics, bytesToHash(topic3)) }
		    input, err := decompress(data)
		    if err != nil { return nil, err }
		    txLogs[txHash] = append(txLogs[txHash], &types.Log{
		      Address: bytesToAddress(address),
		      Topics: topics,
		      Data: input,
		      BlockNumber: blockNumber,
		      TxHash: txHash,
		      Index: logIndex,
		    })
		  }
		  logRows.Close()
		  if err := logRows.Err(); err != nil {
		    return nil, err
		  }

		  rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
		  if err != nil { return nil, err }
		  defer rows.Close()
		  results := []map[string]interface{}{}
		  for rows.Next() {
		    var to, from, blockHash, txHash, contractAddress, bloomBytes []byte
		    var blockNumber, txIndex, gasUsed, cumulativeGasUsed, status, gasPrice  uint64
		    var txTypeRaw sql.NullInt32
		    err := rows.Scan(
		      &blockHash,
		      &blockNumber,
		      &gasUsed,
		      &cumulativeGasUsed,
		      &txHash,
		      &to,
		      &txIndex,
		      &from,
		      &contractAddress,
		      &bloomBytes,
		      &status,
		      &txTypeRaw,
		      &gasPrice,
		    )
		    if err != nil { return nil, err }
		    txType := uint8(txTypeRaw.Int32)
		    logsBloom, err := decompress(bloomBytes)
		    if err != nil { return nil, err }
		    fields := map[string]interface{}{
		      "blockHash":         bytesToHash(blockHash),
		      "blockNumber":       hexutil.Uint64(blockNumber),
		      "transactionHash":   bytesToHash(txHash),
		      "transactionIndex":  hexutil.Uint64(txIndex),
		      "from":              bytesToAddress(from),
		      "to":                bytesToAddressPtr(to),
		      "gasUsed":           hexutil.Uint64(gasUsed),
		      "cumulativeGasUsed": hexutil.Uint64(cumulativeGasUsed),
		      "effectiveGasPrice": hexutil.Uint64(gasPrice),
		      "contractAddress":   nil,
		      "logsBloom":         hexutil.Bytes(logsBloom),
		      "status":            hexutil.Uint(status),
		      "type":              hexutil.Uint(txType),
		    }
		    // If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
		    if address := bytesToAddress(contractAddress); address != (common.Address{}) {
		      fields["contractAddress"] = address
		    }
		    txh := bytesToHash(txHash)
		    for i := range txLogs[txh] {
		      txLogs[txh][i].TxIndex = uint(txIndex)
		      txLogs[txh][i].BlockHash = bytesToHash(blockHash)
		    }
		    logs, ok := txLogs[txh]
				if !ok {
					logs = sortLogs{}
				}
				sort.Sort(logs)
				fields["logs"] = logs
		    results = append(results, fields)
		  }
		  if err := rows.Err(); err != nil { return nil, err }
		  return results, nil
		}

func (api *TransactionAPI) GetTransactionByHash(ctx context.Context, txHash common.Hash) (interface{}, error) {
	var err error
  txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
  if err != nil {
    return nil, err
  }
	if len(txs) == 0 {
		txs, err = api.getPendingTransactions(ctx, 0, 1, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	}
	if err != nil {
    return nil, err
  }
  api.returnSingleTransaction(txs)

	return txs, nil
}

func (api *TransactionAPI) GetTransactionByBlockHashAndIndex(ctx context.Context, txHash common.Hash, index hexutil.Uint64) (interface{}, error) {
	var err error

  txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "blocks.hash = ? AND transactionIndex = ?", trimPrefix(txHash.Bytes()), uint64(index))
  if err != nil {
    return nil, err
  }
  api.returnSingleTransaction(txs)

	return txs, nil
}

func (api *TransactionAPI) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber, index hexutil.Uint64) (interface{}, error) {

  txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "block = ? AND transactionIndex = ?", uint64(blockNumber), uint64(index))
  if err != nil {
    return nil, err
  }
  api.returnSingleTransaction(txs)

	return txs, nil
}

func (api *TransactionAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (interface{}, error) {
	var err error

  receipts, err := api.getTransactionReceiptsBlock(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
  if err != nil {
    return nil, err
  }
  returnSingleReceipt(receipts)

	return receipts, nil
}
