package flumehandler

import (
  "bytes"
  "strings"
  "encoding/json"
  "net/http"
  "database/sql"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/eth/filters"
  "io/ioutil"
  "context"
  "fmt"
  "log"
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
  Result interface{} `json:"result,omitempty"`
}

func formatResponse(result interface{}, call *rpcCall) *rpcResponse {
  return &rpcResponse{
    Version: call.Version,
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
func bytesToHash(data []byte) (common.Hash) {
  result := common.Hash{}
  copy(result[32 - len(data):], data[:])
  return result
}

var (
  fallbackId = json.RawMessage("-1")
)

func GetHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r *http.Request) {
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
    switch call.Method {
    case "eth_getLogs":
      getLogs(r.Context(), w, call, db)
    default:
      handleError(w, "unsupported method", call.ID, 400)
    }
  }
}

func getLatestBlock(ctx context.Context, db *sql.DB) (int64, error) {
  var result int64
  err := db.QueryRowContext(ctx, "SELECT max(blockNumber) FROM event_logs;").Scan(&result)
  return result, err
}

func getLogs(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB) {
  latestBlock, err := getLatestBlock(ctx, db)
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
  params := []interface{}{}
  if crit.BlockHash != nil {
    whereClause = append(whereClause, "blockHash = ?")
    params = append(params, trimPrefix(crit.BlockHash.Bytes()))
  }
  whereClause = append(whereClause, "blockNumber >= ?")
  if crit.FromBlock == nil || crit.FromBlock.Int64() < 0 {
    params = append(params, latestBlock)
  } else {
    params = append(params, crit.FromBlock.Int64())
  }
  whereClause = append(whereClause, "blockNumber <= ?")
  if crit.ToBlock == nil || crit.ToBlock.Int64() < 0{
    params = append(params, latestBlock)
  } else {
    params = append(params, crit.ToBlock.Int64())
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
      topicsClause = append(topicsClause, fmt.Sprintf("topic%v != zeroblob(0)", i))
    }
  }
  if len(topicsClause) > 0 {
    whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(topicsClause, " AND ")))
  }
  query := fmt.Sprintf("SELECT address, topic0, topic1, topic2, topic3, topic4, data, blockNumber, transactionHash, transactionIndex, blockHash, logIndex FROM event_logs WHERE %v;", strings.Join(whereClause, " AND "))
  rows, err := db.QueryContext(ctx, query, params...)
  if err != nil {
    log.Printf("Error selecting: %v - '%v'", err.Error(), query)
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  defer rows.Close()
  logs := []*types.Log{}
  for rows.Next() {
    var address, topic0, topic1, topic2, topic3, topic4, data, transactionHash, blockHash []byte
    var blockNumber uint64
    var transactionIndex, logIndex uint
    err := rows.Scan(&address, &topic0, &topic1, &topic2, &topic3, &topic4, &data, &blockNumber, &transactionHash, &transactionIndex, &blockHash, &logIndex)
    if err != nil {
      handleError(w, err.Error(), call.ID, 500)
      return
    }
    topics := []common.Hash{}
    if len(topic0) > 0 { topics = append(topics, bytesToHash(topic0)) }
    if len(topic1) > 0 { topics = append(topics, bytesToHash(topic1)) }
    if len(topic2) > 0 { topics = append(topics, bytesToHash(topic2)) }
    if len(topic3) > 0 { topics = append(topics, bytesToHash(topic3)) }
    if len(topic4) > 0 { topics = append(topics, bytesToHash(topic4)) }
    logs = append(logs, &types.Log{
      Address: bytesToAddress(address),
      Topics: topics,
      Data: data,
      BlockNumber: blockNumber,
      TxHash: bytesToHash(transactionHash),
      TxIndex: transactionIndex,
      BlockHash: bytesToHash(blockHash),
      Index: logIndex,
    })
  }
  response := formatResponse(logs, call)
  responseBytes, err := json.Marshal(response)
  if err != nil {
    handleError(w, err.Error(), call.ID, 500)
    return
  }
  w.WriteHeader(200)
  w.Write(responseBytes)
}
