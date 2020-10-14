package flumehandler

import (
  "encoding/json"
  "database/sql"
  "math/big"
  "fmt"
  "net/http"
  "strconv"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/openrelayxyz/flume/flumeserver/tokens"
  "log"
)

type txResponse struct {
  BlockNumber string `json:"blockNumber"`
  TimeStamp string `json:"timeStamp"`
  Hash common.Hash `json:"hash"`
  Nonce string `json:"nonce"`
  BlockHash common.Hash `json:"blockHash"`
  TransactionIndex string `json:"transactionIndex"`
  From common.Address `json:"from"`
  To common.Address `json:"to"`
  Value string `json:"value"`
  Gas string `json:"gas"`
  GasPrice string `json:"gasPrice"`
  IsError string `json:"isError"`
  TxReceiptStatus string `json:"txreceipt_status"`
  Input hexutil.Bytes `json:"input"`
  ContractAddress string `json:"contractAddress"`
  CumulativeGasUsed string `json:"cumulativeGasUsed"`
  GasUsed string `json:"gasUsed"`
  Confirmations string `json:"confirmations"`
}

type apiResult struct {
  Status string `json:"status"`
  Message string `json:"message"`
  Result interface{} `json:"result"`
}

func handleApiResponse(w http.ResponseWriter, status int, message string, result interface{}, code int, empty bool) {
  fullResult := &apiResult{
    Status: fmt.Sprintf("%v", status),
    Message: message,
    Result: result,
  }
  if empty {
    fullResult.Status = "0"
    fullResult.Message = "No transactions found"
  }
  res, err := json.Marshal(fullResult)
  if err != nil {
    w.WriteHeader(500)
    w.Write([]byte(`{"status": "0","message":"NOTOK-Marshal Error","result":"Error! Could not serialize result"}\n`))
    return
  }
  w.WriteHeader(code)
  w.Write(res)
  w.Write([]byte("\n"))
}

func GetAPIHandler(db *sql.DB, network uint64) func(http.ResponseWriter, *http.Request) {
  // module=account&action=txlist&address=0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae&startblock=0&endblock=99999999&sort=asc
  return func(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    chainTokens, ok := tokens.Tokens[network]
    if !ok { chainTokens = make(map[common.Address]tokens.Token )}
    switch query.Get("module") + query.Get("action") {
    case "accounttxlist":
      accountTxList(w, r, db)
    case "accounttokentx":
      accountERC20TransferList(w, r, db, chainTokens)
    case "accounttokennfttx":
      accountERC721TransferList(w, r, db, chainTokens)
    case "accountgetminedblocks":
      accountBlocksMined(w, r, db)
    case "blockgetblockcountdown":
      blockCountdown(w, r, db)
    case "blockgetblocknobytime":
      blockByTimestamp(w, r, db)
    default:
      handleApiResponse(w, 0, "NOTOK-invalid action", "Error! Missing or invalid action name", 404, false)
    }
  }
}

func accountTxList(w http.ResponseWriter, r *http.Request, db *sql.DB) {
  query := r.URL.Query()
  if query.Get("address") == "" {
    handleApiResponse(w, 0, "NOTOK-missing arguments", "Error! Missing account address", 400, false)
    return
  }
  addr := common.HexToAddress(query.Get("address"))
  startBlock, _ := strconv.Atoi(query.Get("startblock"))
  endBlock, _ := strconv.Atoi(query.Get("endblock"))
  if endBlock == 0 { endBlock = 99999999}
  page, _ := strconv.Atoi(query.Get("page"))
  offset, _ := strconv.Atoi(query.Get("offset"))
  sort := "ASC"
  if query.Get("sort") == "desc" {
    sort = "DESC"
  }
  if offset == 0 || offset > 10000 { offset = 10000 }
  var headBlockNumber uint64
  err := db.QueryRowContext(r.Context(), "SELECT max(number) FROM blocks;").Scan(&headBlockNumber)
  if headBlockNumber > uint64(endBlock) { endBlock = int(headBlockNumber) }
  rows, err := db.QueryContext(
    r.Context(),
    fmt.Sprintf(`SELECT
      blocks.number, blocks.time, transactions.hash, transactions.nonce, blocks.hash, transactions.transactionIndex, transactions.recipient, transactions.sender, transactions.value, transactions.gas, transactions.gasPrice, transactions.status, transactions.input, transactions.contractAddress, transactions.cumulativeGasUsed, transactions.gasUsed
    FROM transactions
    INNER JOIN blocks on blocks.number = transactions.block
    WHERE (transactions.sender = ? OR transactions.recipient = ?) AND (blocks.number >= ? AND blocks.number <= ?)
    ORDER BY blocks.number %v, transactions.transactionIndex %v
    LIMIT ?
    OFFSET ?;`, sort, sort),
    trimPrefix(addr.Bytes()), trimPrefix(addr.Bytes()), startBlock, endBlock, offset, (page - 1) * offset)
  if err != nil {
    log.Printf("Error querying: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  result := []*txResponse{}
  for rows.Next() {
    var blockNumber uint64
    var blockTime, txNonce, txIndex,  txGas, txGasPrice, txStatus,  txCumulativeGasUsed, txGasUsed string
    var blockHash, txRecipient, txHash, txSender, txValue, txInput, txContractAddress []byte
    if err := rows.Scan(&blockNumber, &blockTime, &txHash, &txNonce, &blockHash, &txIndex, &txRecipient, &txSender, &txValue, &txGas, &txGasPrice, &txStatus, &txInput, &txContractAddress, &txCumulativeGasUsed, &txGasUsed); err != nil {
      log.Printf("Error processing record: %v", err.Error())
      handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
      return
    }
    isError := "0"
    if txStatus == "0" { isError = "1" }
    input, err := decompress(txInput)
    if err != nil {
      log.Printf("Error decompressing record: %v", err.Error())
      handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
      return
    }
    contractAddress := ""
    if addr := bytesToAddress(txContractAddress); addr != (common.Address{}) {
      contractAddress = addr.String()
    }
    tx := &txResponse{
      BlockNumber: fmt.Sprintf("%v", blockNumber),
      TimeStamp: blockTime,
      Hash: bytesToHash(txHash),
      Nonce: txNonce,
      BlockHash: bytesToHash(blockHash),
      TransactionIndex: txIndex,
      From: bytesToAddress(txSender),
      To: bytesToAddress(txRecipient),
      Value: new(big.Int).SetBytes(txValue).String(),
      Gas: txGas,
      GasPrice: txGasPrice,
      TxReceiptStatus: txStatus,
      Input: hexutil.Bytes(input),
      CumulativeGasUsed: txCumulativeGasUsed,
      GasUsed: txGasUsed,
      Confirmations: fmt.Sprintf("%v", (headBlockNumber - blockNumber) + 1),
      IsError: isError,
      ContractAddress: contractAddress,
    }
    result = append(result, tx)
  }
  if err := rows.Err(); err != nil {
    if err != nil {
      log.Printf("Error processing: %v", err.Error())
      handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
      return
    }
  }
  handleApiResponse(w, 1, "OK", result, 200, len(result) == 0)
  return
}

type tokenTransfer struct {
  BlockNumber string `json:"blockNumber"`
  TimeStamp string `json:"timeStamp"`
  Hash common.Hash `json:"hash"`
  Nonce string `json:"nonce"`
  BlockHash common.Hash `json:"blockHash"`
  From common.Address `json:"from"`
  ContractAddress common.Address `json:"contractAddress"`
  To common.Address `json:"to"`
  Value string `json:"value"`
  TokenName string `json:"tokenName"`
  TokenSymbol string `json:"tokenSymbol"`
  TokenDecimal string `json:"tokenDecimal"`
  TransactionIndex string `json:"transactionIndex"`
  Gas string `json:"gas"`
  GasPrice string `json:"gasPrice"`
  GasUsed string `json:"gasUsed"`
  CumulativeGasUsed string `json:"cumulativeGasUsed"`
  Confirmations string `json:"confirmations"`
}

func accountERC20TransferList(w http.ResponseWriter, r *http.Request, db *sql.DB, chainTokens map[common.Address]tokens.Token) {
  accountTokenTransferList(w, r, db, chainTokens, false)
}

func accountERC721TransferList(w http.ResponseWriter, r *http.Request, db *sql.DB, chainTokens map[common.Address]tokens.Token) {
  accountTokenTransferList(w, r, db, chainTokens, true)
}

func accountTokenTransferList(w http.ResponseWriter, r *http.Request, db *sql.DB, chainTokens map[common.Address]tokens.Token, nft bool) {
  query := r.URL.Query()
  if query.Get("address") == "" {
    handleApiResponse(w, 0, "NOTOK-missing arguments", "Error! Missing account address", 400, false)
    return
  }
  addr := common.HexToAddress(query.Get("address"))
  startBlock, _ := strconv.Atoi(query.Get("startblock"))
  endBlock, _ := strconv.Atoi(query.Get("endblock"))
  if endBlock == 0 { endBlock = 99999999}
  page, _ := strconv.Atoi(query.Get("page"))
  offset, _ := strconv.Atoi(query.Get("offset"))
  sort := "ASC"
  if query.Get("sort") == "desc" {
    sort = "DESC"
  }
  if offset == 0 || offset > 10000 { offset = 10000 }
  var headBlockNumber uint64
  err := db.QueryRowContext(r.Context(), "SELECT max(number) FROM blocks;").Scan(&headBlockNumber)
  if headBlockNumber > uint64(endBlock) { endBlock = int(headBlockNumber) }
  topic3Comparison := "="
  if nft { topic3Comparison = "!="}
  rows, err := db.QueryContext(
    r.Context(),
    fmt.Sprintf(`SELECT
      blocks.number, blocks.time, transactions.hash, transactions.nonce, blocks.hash, event_logs.topic1, event_logs.topic2, event_logs.address, event_logs.data, transactions.transactionIndex, transactions.gas, transactions.gasPrice, transactions.input, transactions.cumulativeGasUsed, transactions.gasUsed
    FROM event_logs
    INNER JOIN blocks on blocks.number = event_logs.block
    INNER JOIN transactions on event_logs.tx = transactions.id
    WHERE event_logs.topic0 = X'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' AND (event_logs.topic1 = ? OR event_logs.topic2 = ?) AND event_logs.topic3 %v zeroblob(0) AND (blocks.number >= ? AND blocks.number <= ?)
    ORDER BY blocks.number %v, transactions.transactionIndex %v
    LIMIT ?
    OFFSET ?;`, topic3Comparison, sort, sort),
    trimPrefix(addr.Bytes()), trimPrefix(addr.Bytes()), startBlock, endBlock, offset, (page - 1) * offset)
  if err != nil {
    log.Printf("Error querying: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  result := []*tokenTransfer{}
  for rows.Next() {
    var blockNumber uint64
    var blockTime, txNonce, txIndex,  txGas, txGasPrice, txCumulativeGasUsed, txGasUsed string
    var blockHash, tokenRecipient, txHash, tokenSender, tokenValue, txInput, tokenContractAddress []byte
    if err := rows.Scan(&blockNumber, &blockTime, &txHash, &txNonce, &blockHash, &tokenRecipient, &tokenSender, &tokenContractAddress, &tokenValue, &txIndex, &txGas, &txGasPrice, &txInput, &txCumulativeGasUsed, &txGasUsed); err != nil {
      log.Printf("Error processing record: %v", err.Error())
      handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
      return
    }
    token := chainTokens[bytesToAddress(tokenContractAddress)]
    result = append(result, &tokenTransfer{
      BlockNumber: fmt.Sprintf("%v", blockNumber),
      TimeStamp: blockTime,
      Hash: bytesToHash(txHash),
      Nonce: txNonce,
      BlockHash: bytesToHash(blockHash),
      From: bytesToAddress(tokenSender),
      ContractAddress: bytesToAddress(tokenContractAddress),
      To: bytesToAddress(tokenContractAddress),
      Value: new(big.Int).SetBytes(tokenValue).String(),
      TokenName: token.Name,
      TokenSymbol: token.Symbol,
      TokenDecimal: fmt.Sprintf("%v", token.Decimals),
      TransactionIndex: txIndex,
      Gas: txGas,
      GasPrice: txGasPrice,
      CumulativeGasUsed: txCumulativeGasUsed,
      GasUsed: txGasUsed,
      Confirmations: fmt.Sprintf("%v", (headBlockNumber - blockNumber) + 1),
    })
  }
  if err := rows.Err(); err != nil {
    log.Printf("Error processing: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  handleApiResponse(w, 1, "OK", result, 200, len(result) == 0)
  return

}

type minersBlock struct {
  BlockNumber string `json:"blockNumber"`
  TimeStamp   string `json:"timeStamp"`
  BlockReward string `json:"blockReward"`
  blockReward *big.Int
}

func (b *minersBlock) addGasToReward(gasUsed int64, gasPrice int64) {
  gasCost := new(big.Int).Mul(big.NewInt(gasUsed), big.NewInt(gasPrice))
  b.blockReward = new(big.Int).Add(b.blockReward, gasCost)
  b.BlockReward = b.blockReward.String()
}

func accountBlocksMined(w http.ResponseWriter, r *http.Request, db *sql.DB) {
  query := r.URL.Query()
  if query.Get("address") == "" {
    handleApiResponse(w, 0, "NOTOK-missing arguments", "Error! Missing account address", 400, false)
    return
  }
  addr := common.HexToAddress(query.Get("address"))
  startBlock, _ := strconv.Atoi(query.Get("startblock"))
  endBlock, _ := strconv.Atoi(query.Get("endblock"))
  if endBlock == 0 { endBlock = 99999999}
  page, _ := strconv.Atoi(query.Get("page"))
  offset, _ := strconv.Atoi(query.Get("offset"))
  sort := "ASC"
  if query.Get("sort") == "desc" {
    sort = "DESC"
  }
  if offset == 0 || offset > 10000 { offset = 10000 }
  var headBlockNumber uint64
  err := db.QueryRowContext(r.Context(), "SELECT max(number) FROM blocks;").Scan(&headBlockNumber)
  if headBlockNumber > uint64(endBlock) { endBlock = int(headBlockNumber) }
  rows, err := db.QueryContext(r.Context(),
    fmt.Sprintf(`SELECT
        blocks.number, blocks.time, issuance.value
      FROM blocks
      INNER JOIN issuance on blocks.number > issuance.startBlock AND blocks.number < issuance.endBlock
      WHERE coinbase = ? AND (blocks.number >= ? AND blocks.number <= ?) ORDER BY blocks.number %v LIMIT ? OFFSET ?;`, sort),
    trimPrefix(addr.Bytes()), startBlock, endBlock, offset, (page - 1) * offset)
  if err != nil {
    log.Printf("Error querying: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  minerBlocks := make(map[uint64]*minersBlock)
  result := []*minersBlock{}
  for rows.Next() {
    var blockNumber uint64
    var issuance int64
    var blockTime string
    if err := rows.Scan(&blockNumber, &blockTime, &issuance); err != nil {
      log.Printf("Error getting blocks: %v", err.Error())
      handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
      return
    }
    reward := big.NewInt(issuance)
    minerBlocks[blockNumber] = &minersBlock{
      BlockNumber: fmt.Sprintf("%d", blockNumber),
      TimeStamp: blockTime,
      blockReward: reward,
      BlockReward: reward.String(),
    }
    result = append(result, minerBlocks[blockNumber])
  }
  if err := rows.Err(); err != nil {
    log.Printf("Error processing: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  rows, err = db.QueryContext(r.Context(),
    fmt.Sprintf(`SELECT
        transactions.block, transactions.gasUsed, transactions.gasPrice
      FROM transactions
      WHERE transactions.block in (SELECT blocks.number FROM blocks WHERE coinbase = ? AND (blocks.number >= ? AND blocks.number <= ?) ORDER BY blocks.number %v LIMIT ? OFFSET ?);`, sort),
    trimPrefix(addr.Bytes()), startBlock, endBlock, offset, (page - 1) * offset)
  if err != nil {
    log.Printf("Error querying: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  for rows.Next() {
    var blockNumber uint64
    var gasPrice, gasUsed int64
    if err := rows.Scan(&blockNumber, &gasPrice, &gasUsed); err != nil {
      log.Printf("Error getting fees: %v", err.Error())
      handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
      return
    }
    minerBlocks[blockNumber].addGasToReward(gasUsed, gasPrice)
  }
  if err := rows.Err(); err != nil {
    log.Printf("Error processing: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  handleApiResponse(w, 1, "OK", result, 200, len(result) == 0)
}

type countdown struct {
  CurrentBlock string `json:"CurrentBlock"`
  CountdownBlock string `json:"CountdownBlock"`
  RemainingBlock string `json:"RemainingBlock"`
  EstimateTimeInSec string `json:"EstimateTimeInSec"`
}

// {
//   "CurrentBlock": "11056086",
//   "CountdownBlock": "12056086",
//   "RemainingBlock": "1000000",
//   "EstimateTimeInSec": "13100015.0"
// }


func blockCountdown(w http.ResponseWriter, r *http.Request, db *sql.DB) {
  query := r.URL.Query()
  blockNo, _ := strconv.Atoi(query.Get("blockno"))
  var headBlockNumber int
  err := db.QueryRowContext(r.Context(), "SELECT max(number) FROM blocks;").Scan(&headBlockNumber)
  if err != nil {
    log.Printf("Error querying: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  if blockNo < headBlockNumber {
    handleApiResponse(w, 0, "NOTOK-missing", "Error! Block number already pass", 400, false)
    return
  }
  rows, err := db.QueryContext(r.Context(), `SELECT time FROM blocks ORDER BY number DESC LIMIT 100;`)
  if err != nil {
    log.Printf("Error querying: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  var lastBlock, cumulativeDifference, count int64
  if !rows.Next() {
    log.Printf("Error: No blocks available: %v",)
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  if err := rows.Scan(&lastBlock); err != nil {
    log.Printf("Error scanning: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  for rows.Next() {
    var currentBlock int64
    if err := rows.Scan(&currentBlock); err != nil {
      log.Printf("Error scanning: %v", err.Error())
      handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
      return
    }
    cumulativeDifference += currentBlock - lastBlock
    count++
    lastBlock = currentBlock
  }
  if err := rows.Err(); err != nil {
    log.Printf("Error processing: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  averageBlockTime := float64(cumulativeDifference) / float64(count)

  handleApiResponse(w, 1, "OK", countdown{
    CurrentBlock: fmt.Sprintf("%d", headBlockNumber),
    CountdownBlock: fmt.Sprintf("%d", blockNo),
    RemainingBlock: fmt.Sprintf("%d", blockNo - headBlockNumber),
    EstimateTimeInSec: fmt.Sprintf("%.1f", float64(headBlockNumber - blockNo) * averageBlockTime),
  }, 200, false)
}

func blockByTimestamp(w http.ResponseWriter, r *http.Request, db *sql.DB) {
  query := r.URL.Query()
  timestamp, _ := strconv.Atoi(query.Get("timestamp"))
  sort := "DESC"
  operand := "<="
  if query.Get("closest") == "after" {
    operand = ">="
    sort = "ASC"
  }
  var blockNumber string
  log.Printf(fmt.Sprintf("SELECT number FROM blocks WHERE time %v %v ORDER BY number %v LIMIT 1;", operand, timestamp, sort))
  row := db.QueryRowContext(r.Context(), fmt.Sprintf("SELECT number FROM blocks WHERE time %v ? ORDER BY number %v LIMIT 1;", operand, sort), timestamp)
  if err := row.Scan(&blockNumber); err == sql.ErrNoRows {
    handleApiResponse(w, 0, "NOTOK-missing", "Error! No closest block found", 400, false)
    return
  } else if err != nil {
    log.Printf("Error selecting: %v", err.Error())
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  handleApiResponse(w, 1, "OK", blockNumber, 200, false)
}
