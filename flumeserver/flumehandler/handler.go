// TODO: Check context error before returning results
// TODO: More testing of large result sets for new APIs


package flumehandler

import (
  "sort"
  "bytes"
  "github.com/klauspost/compress/zlib"
  "strings"
  "time"
  "encoding/json"
  "math/big"
  "net/http"
  "database/sql"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/eth/filters"
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/rpc"
  "io/ioutil"
  "io"
  "context"
  "fmt"
  "log"
  "sync"
)

type rpcCall struct {
  Version string `json:"jsonrpc"`
  Method string `json:"method"`
  Params []json.RawMessage `json:"params"`
  ID *json.RawMessage `json:"id"`
}

type rpcResponse struct {
  Version string `json:"jsonrpc"`
  ID *json.RawMessage `json:"id,omitempty"`
  Result interface{} `json:"result"`
}

func formatResponse(result interface{}, call *rpcCall) *rpcResponse {
  return &rpcResponse{
    Version: "2.0",
    ID: call.ID,
    Result: result,
  }
}

func handleError(w http.ResponseWriter, text string, id *json.RawMessage, code int) {
  w.WriteHeader(code)
  w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%v,"error": {"message": "%v"}}`, string(*id), text)))
}

func trimPrefix(data []byte) ([]byte) {
  v := bytes.TrimLeft(data, string([]byte{0}))
  if len(v) == 0 {
    return []byte{0}
  }
  return v
}

func bytesToAddress(data []byte) (common.Address) {
  result := common.Address{}
  copy(result[20 - len(data):], data[:])
  return result
}
func bytesToAddressPtr(data []byte) (*common.Address) {
  if len(data) == 0 { return nil }
  result := bytesToAddress(data)
  return &result
}
func bytesToHash(data []byte) (common.Hash) {
  result := common.Hash{}
  copy(result[32 - len(data):], data[:])
  return result
}

var (
  fallbackId = json.RawMessage("-1")
)

func GetHandler(db *sql.DB, chainid uint64, wg *sync.WaitGroup) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r *http.Request) {
    if r.Method == "GET" {
      if _, err := getLatestBlock(r.Context(), db, w); err != nil {
        w.WriteHeader(500)
        w.Write([]byte(`{"ok":false}`))
        log.Printf("Unhealthy: Error getting latest block: %v", err.Error())
        return
      }
      w.WriteHeader(200)
      w.Write([]byte(`{"ok":true}`))
      return
    }
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      handleError(w, "error reading body", &fallbackId, 400)
      return
    }
    call := &rpcCall{}
    if err := json.Unmarshal(body, call); err != nil {
      handleError(w, "error reading body", &fallbackId, 400)
      return
    }
    wg.Wait()
    switch call.Method {
    case "eth_getLogs":
      getLogs(r.Context(), w, call, db, chainid)
    case "eth_blockNumber":
      getBlockNumber(r.Context(), w, call, db, chainid)
    case "eth_getTransactionByHash":
      getTransactionByHash(r.Context(), w, call, db, chainid)
    case "eth_getTransactionByBlockHashAndIndex":
      getTransactionByBlockHashAndIndex(r.Context(), w, call, db, chainid)
    case "eth_getTransactionByBlockNumberAndIndex":
      getTransactionByBlockNumberAndIndex(r.Context(), w, call, db, chainid)
    case "eth_getTransactionReceipt":
      getTransactionReceipt(r.Context(), w, call, db, chainid)
    case "eth_getBlockByNumber":
      getBlockByNumber(r.Context(), w, call, db, chainid)
    case "eth_getBlockByHash":
      getBlockByHash(r.Context(), w, call, db, chainid)
    case "eth_getBlockTransactionCountByNumber":
      getBlockTransactionCountByNumber(r.Context(), w, call, db, chainid)
    case "eth_getBlockTransactionCountByHash":
      getBlockTransactionCountByHash(r.Context(), w, call, db, chainid)
    case "eth_getUncleCountByBlockNumber":
      getUncleCountByBlockNumber(r.Context(), w, call, db, chainid)
    case "eth_getUncleCountByBlockHash":
      getUncleCountByBlockHash(r.Context(), w, call, db, chainid)
    case "eth_gasPrice":
      gasPrice(r.Context(), w, call, db, chainid)
    case "eth_feeHistory":
      feeHistory(r.Context(), w, call, db, chainid)
    case "eth_maxPriorityFeePerGas":
      maxPriorityFeePerGas(r.Context(), w, call, db, chainid)
    case "eth_getTransactionCount":
      getTransactionCount(r.Context(), w, call, db, chainid)
    case "flume_erc20ByAccount":
      getERC20ByAccount(r.Context(), w, call, db, chainid)
    case "flume_erc20Holders":
      getERC20Holders(r.Context(), w, call, db, chainid)
    case "flume_getTransactionsBySender":
      getTransactionsBySender(r.Context(), w, call, db, chainid)
    case "flume_getTransactionReceiptsBySender":
      getTransactionReceiptsBySender(r.Context(), w, call, db, chainid)
    case "flume_getTransactionsByRecipient":
      getTransactionsByRecipient(r.Context(), w, call, db, chainid)
    case "flume_getTransactionReceiptsByRecipient":
      getTransactionReceiptsByRecipient(r.Context(), w, call, db, chainid)
    case "flume_getTransactionsByParticipant":
      getTransactionsByParticipant(r.Context(), w, call, db, chainid)
    case "flume_getTransactionReceiptsByParticipant":
      getTransactionReceiptsByParticipant(r.Context(), w, call, db, chainid)
    case "flume_getTransactionReceiptsByBlockHash":
      getTransactionReceiptsByBlockHash(r.Context(), w, call, db, chainid)
    case "flume_getTransactionReceiptsByBlockNumber":
      getTransactionReceiptsByBlockNumber(r.Context(), w, call, db, chainid)
    default:
      handleError(w, "unsupported method", call.ID, 400)
    }
    w.Write([]byte("\n"))
  }
}

func getLatestBlock(ctx context.Context, db *sql.DB, w http.ResponseWriter) (int64, error) {
  var result int64
  var hash []byte
  err := db.QueryRowContext(ctx, "SELECT max(number), hash FROM blocks;").Scan(&result, &hash)
  if w != nil {
    w.Header().Set("X-Hash", fmt.Sprintf("%#x", hash))
  }
  return result, err
}

func getBlockNumber(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  blockNo, err := getLatestBlock(ctx, db, w)
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  responseBytes, err := json.Marshal(formatResponse(hexutil.Uint64(blockNo), call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
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

func getLogs(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  latestBlock, err := getLatestBlock(ctx, db, w)
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  crit := filters.FilterCriteria{}
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  if err := json.Unmarshal(call.Params[0], &crit); err != nil {
    log.Printf("Error unmarshalling into criteria: %v", err.Error())
    handleError(w, err.Error(), call.ID, 400)
    return
  }
  whereClause := []string{}
  indexClause := ""
  params := []interface{}{}
  if crit.BlockHash != nil {
    var num int64
    db.QueryRowContext(ctx, "SELECT number FROM blocks WHERE hash = ?", crit.BlockHash.Bytes()).Scan(&num)
    whereClause = append(whereClause, "blockHash = ? AND block = ?")
    params = append(params, trimPrefix(crit.BlockHash.Bytes()), num)
  } else {
    var fromBlock, toBlock int64
    if crit.FromBlock == nil || crit.FromBlock.Int64() < 0 {
      fromBlock = latestBlock
    } else {
      fromBlock = crit.FromBlock.Int64()
    }
    whereClause = append(whereClause, "block >= ?")
    params = append(params, fromBlock)
    if crit.ToBlock == nil || crit.ToBlock.Int64() < 0{
      toBlock = latestBlock
    } else {
      toBlock = crit.ToBlock.Int64()
    }
    whereClause = append(whereClause, "block <= ?")
    params = append(params, toBlock)
  }
  addressClause := []string{}
  for _, address := range crit.Addresses {
    addressClause = append(addressClause, "address = ?")
    params = append(params, trimPrefix(address.Bytes()))
  }
  if len(addressClause) > 0 {
    whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(addressClause, " OR ")))
  }
  topicsClause := []string{}
  for i, topics := range crit.Topics {
    topicClause := []string{}
    for _, topic := range topics {
      topicClause = append(topicClause, fmt.Sprintf("topic%v = ?", i))
      params = append(params, trimPrefix(topic.Bytes()))
    }
    if len(topicClause) > 0 {
      topicsClause = append(topicsClause, fmt.Sprintf("(%v)", strings.Join(topicClause, " OR ")))
    } else {
      topicsClause = append(topicsClause, fmt.Sprintf("topic%v IS NOT NULL", i))
    }
  }
  if len(topicsClause) > 0 {
    whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(topicsClause, " AND ")))
  }
  query := fmt.Sprintf("SELECT address, topic0, topic1, topic2, topic3, data, block, transactionHash, transactionIndex, blockHash, logIndex FROM event_logs %v WHERE %v;", indexClause, strings.Join(whereClause, " AND "))
  rows, err := db.QueryContext(ctx, query, params...)
  if err != nil {
    log.Printf("Error selecting: %v - '%v'", err.Error(), query)
    handleError(w, "database error", call.ID, 500)
    return
  }
  defer rows.Close()
  logs := sortLogs{}
  blockNumbersInResponse := make(map[uint64]struct{})
  for rows.Next() {
    var address, topic0, topic1, topic2, topic3, data, transactionHash, blockHash []byte
    var blockNumber uint64
    var transactionIndex, logIndex uint
    err := rows.Scan(&address, &topic0, &topic1, &topic2, &topic3, &data, &blockNumber, &transactionHash, &transactionIndex, &blockHash, &logIndex)
    if err != nil {
      log.Printf("Error scanning: %v", err.Error())
      handleError(w, "database error", call.ID, 500)
      return
    }
    blockNumbersInResponse[blockNumber] = struct{}{}
    topics := []common.Hash{}
    if len(topic0) > 0 { topics = append(topics, bytesToHash(topic0)) }
    if len(topic1) > 0 { topics = append(topics, bytesToHash(topic1)) }
    if len(topic2) > 0 { topics = append(topics, bytesToHash(topic2)) }
    if len(topic3) > 0 { topics = append(topics, bytesToHash(topic3)) }
    input, err := decompress(data)
    if err != nil {
      log.Printf("Error decompressing data: %v", err.Error())
      handleError(w, "database error", call.ID, 500)
      return
    }
    logs = append(logs, &types.Log{
      Address: bytesToAddress(address),
      Topics: topics,
      Data: input,
      BlockNumber: blockNumber,
      TxHash: bytesToHash(transactionHash),
      TxIndex: transactionIndex,
      BlockHash: bytesToHash(blockHash),
      Index: logIndex,
    })
    if len(logs) > 10000 && len(blockNumbersInResponse) > 1 {
      handleError(w, "query returned more than 10,000 results spanning multiple blocks", call.ID, 413)
      return
    }
  }
  if err := rows.Err(); err != nil {
    log.Printf("Error scanning: %v", err.Error())
    handleError(w, "database error", call.ID, 500)
    return
  }
  sort.Sort(logs)
  responseBytes, err := json.Marshal(formatResponse(logs, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

type paginator struct {
  Items interface{} `json:"items"`
  Token interface{} `json:"next,omitempty"`
}

func getERC20ByAccount(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 || len(call.Params) > 2 {
    handleError(w, "expected 1 - 2 parameters", call.ID, 400)
    return
  }
  addr := &common.Address{}
  if err := json.Unmarshal(call.Params[0], addr); err != nil {
    handleError(w, err.Error(), call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }

  tctx, cancel := context.WithTimeout(ctx, 5 * time.Second)
  defer cancel()

  topic0 := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
  // topic0 must match ERC20, topic3 must be empty (to exclude ERC721) and topic2 is the recipient address
  rows, err := db.QueryContext(tctx, `SELECT distinct(address) FROM event_logs INDEXED BY topic2_partial WHERE topic0 = ? AND topic2 = ? AND topic3 IS NULL LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), offset)
  if err != nil {
    log.Printf("Error getting account addresses: %v", err.Error())
    handleError(w, "database error", call.ID, 500)
    return
  }
  defer rows.Close()
  addresses := []common.Address{}
  for rows.Next() {
    var addrBytes []byte
    err := rows.Scan(&addrBytes)
    if err != nil {
      log.Printf("Error scanning: %v", err.Error())
      handleError(w, "database error", call.ID, 500)
      return
    }
    addresses = append(addresses, bytesToAddress(addrBytes))
  }
  if err := rows.Err(); err != nil {
    log.Printf("Error scanning: %v", err.Error())
    handleError(w, "database error", call.ID, 500)
    return
  }
  result := paginator{Items: addresses}
  if len(addresses) == 1000 {
    result.Token = offset + len(addresses)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getERC20Holders(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 || len(call.Params) > 2 {
    handleError(w, "expected 1 - 2 parameters", call.ID, 400)
    return
  }
  addr := &common.Address{}
  if err := json.Unmarshal(call.Params[0], addr); err != nil {
    handleError(w, err.Error(), call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }

  tctx, cancel := context.WithTimeout(ctx, 5 * time.Second)
  defer cancel()

  topic0 := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
  // topic0 must match ERC20, topic3 must be empty (to exclude ERC721) and topic2 is the recipient address
  rows, err := db.QueryContext(tctx, `SELECT distinct(topic2) FROM event_logs INDEXED BY address_compound WHERE topic0 = ? AND address = ? AND topic3 IS NULL LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), offset)
  if err != nil {
    log.Printf("Error getting account addresses: %v", err.Error())
    handleError(w, "database error", call.ID, 500)
  }
  defer rows.Close()
  addresses := []common.Address{}
  for rows.Next() {
    var addrBytes []byte
    err := rows.Scan(&addrBytes)
    if err != nil {
      log.Printf("Error scanning: %v", err.Error())
      handleError(w, "database error", call.ID, 500)
      return
    }
    addresses = append(addresses, bytesToAddress(addrBytes))
  }
  if err := rows.Err(); err != nil {
    log.Printf("Error scanning: %v", err.Error())
    handleError(w, "database error", call.ID, 500)
    return
  }
  result := paginator{Items: addresses}
  if len(addresses) == 1000 {
    result.Token = offset + len(addresses)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    log.Printf("Error scanning: %v", err.Error())
    handleError(w, "database error", call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
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

func uintToHexBig(a uint64) *hexutil.Big {
  x := hexutil.Big(*new(big.Int).SetUint64(a))
  return &x
}

func bytesToHexBig(a []byte) *hexutil.Big {
  x := hexutil.Big(*new(big.Int).SetBytes(a))
  return &x
}

func decompress(data []byte) ([]byte, error) {
  if len(data) == 0 { return data, nil }
  r, err := zlib.NewReader(bytes.NewBuffer(data))
  if err != nil { return []byte{}, err }
  raw, err := ioutil.ReadAll(r)
  if err == io.EOF || err == io.ErrUnexpectedEOF {
    return raw, nil
  }
  return raw, err
}

func deriveChainID(x uint64) *hexutil.Big {
  v := new(big.Int).SetUint64(x)
  if v.BitLen() <= 64 {
		if x == 27 || x == 28 {
			return nil
		}
    y := hexutil.Big(*new(big.Int).SetUint64((x - 35) / 2))
		return &y
	}
	v = new(big.Int).Sub(v, big.NewInt(35))
  y := hexutil.Big(*v.Div(v, big.NewInt(2)))
	return &y
}

// getTransactionsBlock returns zero or more transactions matching the
// whereClause. This should be used when all returned transactions can be
// expected to come from a single block, otherwise the transactions may not be
// sorted correctly. This will return the same transactions as getTransactions,
// but is more efficient when the transactions can be limited to a single
// block.
func getTransactionsBlock(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]*rpcTransaction, error) {
	query := fmt.Sprintf("SELECT blocks.hash, block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap FROM transactions INNER JOIN blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.transactionIndex LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
}

// getTransaction returns zero or more transactions matching the whereClause
// sorted by blockNumber, transactionIndex
func getTransactions(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]*rpcTransaction, error) {
	query := fmt.Sprintf("SELECT blocks.hash, block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap FROM transactions INNER JOIN blocks ON blocks.number = transactions.block WHERE transactions.rowid IN (SELECT transactions.rowid FROM transactions INNER JOIN blocks ON transactions.block = blocks.number WHERE %v) LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
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

func returnSingleTransaction(txs []*rpcTransaction, w http.ResponseWriter, call *rpcCall) {
  var result interface{}
  if len(txs) > 0 {
    result = txs[0]
  } else {
    result = nil
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
func returnSingleReceipt(txs []map[string]interface{}, w http.ResponseWriter, call *rpcCall) {
  var result interface{}
  if len(txs) > 0 {
    result = txs[0]
  } else {
    result = nil
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getTransactionByHash(ctx context.Context ,w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var txHash common.Hash
  if err := json.Unmarshal(call.Params[0], &txHash); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  txs, err := getTransactionsBlock(ctx, db, 0, 1, chainid, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
  if err != nil {
    log.Printf("Error getting transactions: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
	if len(txs) == 0 {
		txs, err = getPendingTransactions(ctx, db, 0, 1, chainid, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	}
	if err != nil {
    log.Printf("Error getting mempool transactions: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  returnSingleTransaction(txs, w, call)
}

func getTransactionByBlockHashAndIndex(ctx context.Context ,w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 2 {
    handleError(w, "missing value for required argument 1", call.ID, 400)
    return
  }
  var txHash common.Hash
  if err := json.Unmarshal(call.Params[0], &txHash); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  var index hexutil.Uint64
  if err := json.Unmarshal(call.Params[1], &index); err != nil {
    handleError(w, "error reading params.1", call.ID, 400)
    return
  }
  txs, err := getTransactionsBlock(ctx, db, 0, 1, chainid, "blocks.hash = ? AND transactionIndex = ?", trimPrefix(txHash.Bytes()), uint64(index))
  if err != nil {
    log.Printf("Error getting transactions: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  returnSingleTransaction(txs, w, call)
}

func getTransactionByBlockNumberAndIndex(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 2 {
    handleError(w, "missing value for required argument 1", call.ID, 400)
    return
  }
  var number, index hexutil.Uint64
  if err := json.Unmarshal(call.Params[0], &number); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  if err := json.Unmarshal(call.Params[1], &index); err != nil {
    handleError(w, "error reading params.1", call.ID, 400)
    return
  }
  txs, err := getTransactionsBlock(ctx, db, 0, 1, chainid, "block = ? AND transactionIndex = ?", uint64(number), uint64(index))
  if err != nil {
    log.Printf("Error getting transactions: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  returnSingleTransaction(txs, w, call)
}

func getTransactionReceipt(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var txHash common.Hash
  if err := json.Unmarshal(call.Params[0], &txHash); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  receipts, err := getTransactionReceiptsBlock(ctx, db, 0, 1, chainid, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
  if err != nil {
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  returnSingleReceipt(receipts, w, call)
}

func getTransactionsBySender(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var address common.Address
  if err := json.Unmarshal(call.Params[0], &address); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }
  txs, err := getPendingTransactions(ctx, db, offset, 1000, chainid, "sender = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting pending txs: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  ctxs, err := getTransactions(ctx, db, offset, 1000, chainid, "sender = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting txs: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  txs = append(txs, ctxs...)
  result := paginator{Items: txs}
  if len(txs) >= 1000 {
    result.Token = offset + len(txs)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getTransactionReceiptsBySender(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var address common.Address
  if err := json.Unmarshal(call.Params[0], &address); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }
  receipts, err := getTransactionReceipts(ctx, db, offset, 1000, chainid, "sender = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  result := paginator{Items: receipts}
  if len(receipts) == 1000 {
    result.Token = offset + len(receipts)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
func getTransactionsByRecipient(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var address common.Address
  if err := json.Unmarshal(call.Params[0], &address); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }
  txs, err := getPendingTransactions(ctx, db, offset, 1000, chainid, "recipient = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting pending txs: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  ctxs, err := getTransactions(ctx, db, offset, 1000, chainid, "recipient = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting txs: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  txs = append(txs, ctxs...)
  result := paginator{Items: txs}
  if len(txs) >= 1000 {
    result.Token = offset + len(txs)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getTransactionReceiptsByRecipient(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var address common.Address
  if err := json.Unmarshal(call.Params[0], &address); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }
  receipts, err := getTransactionReceipts(ctx, db, offset, 1000, chainid, "recipient = ?", trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  result := paginator{Items: receipts}
  if len(receipts) == 1000 {
    result.Token = offset + len(receipts)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
func getTransactionsByParticipant(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var address common.Address
  if err := json.Unmarshal(call.Params[0], &address); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }
  txs, err := getPendingTransactions(ctx, db, offset, 1000, chainid, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting pending txs: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  ctxs, err := getTransactions(ctx, db, offset, 1000, chainid, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting txs: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  txs = append(txs, ctxs...)
  result := paginator{Items: txs}
  if len(txs) >= 1000 {
    result.Token = offset + len(txs)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getTransactionReceiptsByParticipant(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var address common.Address
  if err := json.Unmarshal(call.Params[0], &address); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  offset := 0
  if len(call.Params) > 1 {
    if err := json.Unmarshal(call.Params[1], &offset); err != nil {
      handleError(w, err.Error(), call.ID, 400)
      return
    }
  }
  receipts, err := getTransactionReceipts(ctx, db, offset, 1000, chainid, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  result := paginator{Items: receipts}
  if len(receipts) == 1000 {
    result.Token = offset + len(receipts)
  }
  responseBytes, err := json.Marshal(formatResponse(result, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
func getTransactionReceiptsByBlockHash(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var blockHash common.Hash
  if err := json.Unmarshal(call.Params[0], &blockHash); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  // Offset and limit aren't too relevant here, as there's a limit on
  // transactions per block.
  receipts, err := getTransactionReceiptsBlock(ctx, db, 0, 100000, chainid, "blocks.hash = ?", trimPrefix(blockHash.Bytes()))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  responseBytes, err := json.Marshal(formatResponse(receipts, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
func getTransactionReceiptsByBlockNumber(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var blockNumber hexutil.Uint64
  if err := json.Unmarshal(call.Params[0], &blockNumber); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  receipts, err := getTransactionReceiptsBlock(ctx, db, 0, 100000, chainid, "block = ?", uint64(blockNumber))
  if err != nil {
    log.Printf("Error getting receipts: %v", err.Error())
    handleError(w, "error reading database", call.ID, 400)
    return
  }
  responseBytes, err := json.Marshal(formatResponse(receipts, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
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

func getBlockByNumber(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 2 {
    handleError(w, "missing value for required argument 1", call.ID, 400)
    return
  }
  var blockNumber rpc.BlockNumber
  if err := json.Unmarshal(call.Params[0], &blockNumber); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  if blockNumber.Int64() < 0 {
    latestBlock, err := getLatestBlock(ctx, db, w)
    if err != nil {
      handleError(w, err.Error(), call.ID, 500)
      return
    }
    blockNumber = rpc.BlockNumber(latestBlock)
  }
  var includeTxs bool
  if err := json.Unmarshal(call.Params[1], &includeTxs); err != nil {
    handleError(w, "error reading params.1", call.ID, 400)
    return
  }
  blocks, err := getBlocks(ctx, db, includeTxs, chainid, "number = ?", blockNumber)
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  var blockVal interface{}
  if len(blocks) > 0 {
    blockVal = blocks[0]
  }
  responseBytes, err := json.Marshal(formatResponse(blockVal, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getBlockByHash(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 2 {
    handleError(w, "missing value for required argument 1", call.ID, 400)
    return
  }
  var blockHash common.Hash
  if err := json.Unmarshal(call.Params[0], &blockHash); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  var includeTxs bool
  if err := json.Unmarshal(call.Params[1], &includeTxs); err != nil {
    handleError(w, "error reading params.1", call.ID, 400)
    return
  }
  blocks, err := getBlocks(ctx, db, includeTxs, chainid, "hash = ?", trimPrefix(blockHash.Bytes()))
  var blockVal interface{}
  if len(blocks) > 0 {
    blockVal = blocks[0]
  }
  responseBytes, err := json.Marshal(formatResponse(blockVal, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func txCount(ctx context.Context, db *sql.DB, whereClause string, params ...interface{}) (hexutil.Uint64, error) {
  var count uint64
  err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM transactions WHERE %v", whereClause), params...).Scan(&count)
  return hexutil.Uint64(count), err
}
func getSenderNonce(ctx context.Context, db *sql.DB, sender common.Address) (hexutil.Uint64, error) {
  var count, nonce uint64
  if err := db.QueryRowContext(ctx, "SELECT count(*) FROM transactions WHERE sender = ?", trimPrefix(sender.Bytes())).Scan(&count); err != nil {
    return 0, err
  }
  if err := db.QueryRowContext(ctx, "SELECT max(nonce) FROM mempool.transactions WHERE sender = ?", trimPrefix(sender.Bytes())).Scan(&count); err != nil {
    return 0, err
  }
  if nonce > count {
    return hexutil.Uint64(nonce), nil
  }
  return hexutil.Uint64(count), nil
}

func getBlockTransactionCountByNumber(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var blockNumber rpc.BlockNumber
  if err := json.Unmarshal(call.Params[0], &blockNumber); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  if blockNumber.Int64() < 0 {
    latestBlock, err := getLatestBlock(ctx, db, w)
    if err != nil {
      handleError(w, err.Error(), call.ID, 500)
      return
    }
    blockNumber = rpc.BlockNumber(latestBlock)
  }
  count, err := txCount(ctx, db, "block = ?", blockNumber)
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  responseBytes, err := json.Marshal(formatResponse(count, call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getBlockTransactionCountByHash(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var blockHash common.Hash
  if err := json.Unmarshal(call.Params[0], &blockHash); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  var count uint64
  if err := db.QueryRowContext(ctx, "SELECT count(*) FROM v_transactions WHERE blockHash = ?", trimPrefix(blockHash.Bytes())).Scan(&count); err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  responseBytes, err := json.Marshal(formatResponse(hexutil.Uint64(count), call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getUncleCountByBlockNumber(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var blockNumber rpc.BlockNumber
  if err := json.Unmarshal(call.Params[0], &blockNumber); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  if blockNumber.Int64() < 0 {
    latestBlock, err := getLatestBlock(ctx, db, w)
    if err != nil {
      handleError(w, err.Error(), call.ID, 500)
      return
    }
    blockNumber = rpc.BlockNumber(latestBlock)
  }
  var uncles []byte
  if err := db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE number = ?", blockNumber).Scan(&uncles); err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  unclesList := []common.Hash{}
  rlp.DecodeBytes(uncles, &unclesList)
  responseBytes, err := json.Marshal(formatResponse(hexutil.Uint64(len(unclesList)), call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

func getUncleCountByBlockHash(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
  if len(call.Params) < 1 {
    handleError(w, "missing value for required argument 0", call.ID, 400)
    return
  }
  var blockHash common.Hash
  if err := json.Unmarshal(call.Params[0], &blockHash); err != nil {
    handleError(w, "error reading params.0", call.ID, 400)
    return
  }
  var uncles []byte
  if err := db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE hash = ?", trimPrefix(blockHash.Bytes())).Scan(&uncles); err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  unclesList := []common.Hash{}
  rlp.DecodeBytes(uncles, &unclesList)
  responseBytes, err := json.Marshal(formatResponse(hexutil.Uint64(len(unclesList)), call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}

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

func gasTip(ctx context.Context, db *sql.DB, w http.ResponseWriter) (*big.Int, error) {
  latestBlock, err := getLatestBlock(ctx, db, w)
  if err != nil {
    return nil, err
  }
  rows, err := db.QueryContext(ctx, "SELECT gasPrice, baseFee from transactions INNER JOIN blocks ON transactions.block = blocks.number WHERE blocks.number > ?;", latestBlock - 20)
  if err != nil {
    return nil, err
  }
  defer rows.Close()
	tips := bigList{}
  for rows.Next() {
		var gasPrice int64
		var baseFeeBytes []byte
		if err := rows.Scan(&gasPrice, &baseFeeBytes); err != nil {
			return nil, err
		}
		tip := new(big.Int).Sub(big.NewInt(gasPrice), new(big.Int).SetBytes(baseFeeBytes))
		if tip.Cmp(new(big.Int)) > 0 {
			// Leave out transactions without tips, as these tend to be MEV
			// transactions
			tips = append(tips, tip)
		}
  }
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(tips) > 0 {
		sort.Sort(tips)
		return tips[(len(tips) * 6) / 10], nil
	}
	// If there are no transactions in the last 20 blocks, just look at the
	// latest transaction.
	var gasPrice int64
	var baseFeeBytes []byte
	err = db.QueryRowContext(ctx, "SELECT gasPrice, baseFee from transactions INNER JOIN blocks ON transactions.block = blocks.number WHERE 1 ORDER BY id DESC LIMIT 1;").Scan(&gasPrice, &baseFeeBytes)
	return new(big.Int).Sub(big.NewInt(gasPrice), new(big.Int).SetBytes(baseFeeBytes)), err
}

func nextBaseFee(ctx context.Context, db *sql.DB) (*big.Int, error) {
	var baseFeeBytes []byte
	var gasLimit, gasUsed int64
	err := db.QueryRowContext(ctx, "SELECT baseFee, gasUsed, gasLimit FROM blocks ORDER BY number DESC LIMIT 1;").Scan(&baseFeeBytes, &gasUsed, &gasLimit)
	if err != nil {
		return nil, err
	}
	baseFee := new(big.Int).SetBytes(baseFeeBytes)
	gasTarget := gasLimit / 2
	if gasUsed == gasTarget {
		return baseFee, nil
	} else if gasUsed > gasTarget {
		delta := gasUsed - gasTarget
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(baseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), big.NewInt(8))
		if baseFeeDelta.Cmp(new(big.Int)) == 0 {
			baseFeeDelta = big.NewInt(1)
		}
		return new(big.Int).Add(baseFee, baseFeeDelta), nil
	}
	delta := gasTarget - gasUsed
	baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(baseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), big.NewInt(8))
	return new(big.Int).Sub(baseFee, baseFeeDelta), nil
}

func gasPrice(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
	tip, err := gasTip(ctx, db, w)
	if err != nil {
		handleError(w, err.Error(), call.ID, 500)
		return
	}
	baseFee, err := nextBaseFee(ctx, db)
	if err != nil {
		handleError(w, err.Error(), call.ID, 500)
		return
	}

  responseBytes, err := json.Marshal(formatResponse((*hexutil.Big)(new(big.Int).Add(tip, baseFee)), call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
func maxPriorityFeePerGas(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
	tip, err := gasTip(ctx, db, w)
	if err != nil {
		handleError(w, err.Error(), call.ID, 500)
		return
	}
	responseBytes, err := json.Marshal(formatResponse((*hexutil.Big)(tip), call))
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
func getTransactionCount(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
	if len(call.Params) < 1 {
		handleError(w, "missing value for required argument 0", call.ID, 400)
		return
	}
	var addr common.Address
	if err := json.Unmarshal(call.Params[0], &addr); err != nil {
		handleError(w, "error reading params.0", call.ID, 400)
		return
	}
	nonce, err := getSenderNonce(ctx, db, addr)
	if err != nil {
		handleError(w, err.Error(), call.ID, 500)
		return
	}
	responseBytes, err := json.Marshal(formatResponse(nonce, call))
	if err != nil {
		handleError(w, err.Error(), call.ID, 500)
		return
	}
	w.WriteHeader(200)
	w.Write(responseBytes)
}
