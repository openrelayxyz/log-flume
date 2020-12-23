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
  "strings"
)

type txResponse struct {
  BlockNumber string `json:"blockNumber"`
  TimeStamp string `json:"timeStamp"`
  Hash common.Hash `json:"hash"`
  Nonce string `json:"nonce"`
  BlockHash common.Hash `json:"blockHash"`
  TransactionIndex string `json:"transactionIndex"`
  From common.Address `json:"from"`
  To string `json:"to"`
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

func handleApiError(err error, w http.ResponseWriter, msg, result, logMsg string, code int) bool {
  if err != nil {
    log.Printf("%v: %v", logMsg, err.Error())
    handleApiResponse(w, 0, fmt.Sprintf("NOTOK-%v", msg), result, code, false)
    return true
  }
  return false
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
    if !ok {
      log.Printf("No tokens for network %v - making empty map", network)
      chainTokens = make(map[common.Address]tokens.Token )
    }
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
    case "tokentokeninfo":
      getTokenInfo(w, r, db, chainTokens)
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
      transactions.block, blocks.time, transactions.hash, transactions.nonce, blocks.hash, transactions.transactionIndex, transactions.recipient, transactions.sender, transactions.value, transactions.gas, transactions.gasPrice, transactions.status, transactions.input, transactions.contractAddress, transactions.cumulativeGasUsed, transactions.gasUsed
    FROM transactions
    INNER JOIN blocks on blocks.number = transactions.block
    WHERE transactions.rowid in (
      SELECT rowid FROM transactions WHERE sender = ? AND (block >= ? AND block <= ?)
      UNION SELECT rowid FROM transactions WHERE recipient = ? AND (block >= ? AND block <= ?)
      UNION SELECT rowid FROM transactions WHERE contractAddress = ? AND (block >= ? AND block <= ?)
      ORDER BY rowid %v LIMIT ? OFFSET ?
    ) ORDER BY transactions.block %v, transactions.transactionIndex %v;`, sort, sort, sort),
    trimPrefix(addr.Bytes()), startBlock, endBlock, trimPrefix(addr.Bytes()), startBlock, endBlock, trimPrefix(addr.Bytes()), startBlock, endBlock, offset, (page - 1) * offset)
  if handleApiError(err, w, "database error", "Error! Database error", "Error querying", 500) { return }
  result := []*txResponse{}
  for rows.Next() {
    var blockNumber uint64
    var blockTime, txNonce, txIndex,  txGas, txGasPrice, txStatus,  txCumulativeGasUsed, txGasUsed string
    var blockHash, txRecipient, txHash, txSender, txValue, txInput, txContractAddress []byte
    err := rows.Scan(&blockNumber, &blockTime, &txHash, &txNonce, &blockHash, &txIndex, &txRecipient, &txSender, &txValue, &txGas, &txGasPrice, &txStatus, &txInput, &txContractAddress, &txCumulativeGasUsed, &txGasUsed)
    if handleApiError(err, w, "database error", "Error! Database error", "Error processing", 500) { return }
    isError := "1"
    if txStatus == "0" {
      isError = "0"
      txStatus = ""
    }
    input, err := decompress(txInput)
    if handleApiError(err, w, "database error", "Error! Database error", "Error decompressing", 500) { return }
    contractAddress := ""
    if addr := bytesToAddress(txContractAddress); addr != (common.Address{}) {
      contractAddress = addr.String()
    }
    to := bytesToAddress(txRecipient)
    tx := &txResponse{
      BlockNumber: fmt.Sprintf("%v", blockNumber),
      TimeStamp: blockTime,
      Hash: bytesToHash(txHash),
      Nonce: txNonce,
      BlockHash: bytesToHash(blockHash),
      TransactionIndex: txIndex,
      From: bytesToAddress(txSender),
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
    if len(txRecipient) > 0 {
      tx.To = to.String()
    }
    result = append(result, tx)
  }
  if handleApiError(rows.Err(), w, "database error", "Error! Database error", "Error processing", 500) { return }
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
  Value string `json:"value,omitempty"`
  TokenID *common.Hash `json:"tokenID,omitempty"`
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
      blocks.number, blocks.time, transactions.hash, transactions.nonce, blocks.hash, event_logs.topic1, event_logs.topic2, event_logs.topic3, event_logs.address, event_logs.data, transactions.transactionIndex, transactions.gas, transactions.gasPrice, transactions.input, transactions.cumulativeGasUsed, transactions.gasUsed
    FROM event_logs NOT INDEXED
    INNER JOIN blocks on blocks.number = event_logs.block
    INNER JOIN transactions on event_logs.tx = transactions.id
    WHERE
      event_logs.rowid IN (
        SELECT rowid FROM event_logs INDEXED BY topic1 WHERE event_logs.topic0 = X'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' AND event_logs.topic1 = ? AND event_logs.topic3 %v zeroblob(0) AND (block >= ? AND block <= ?)
        UNION SELECT rowid FROM event_logs INDEXED BY topic2 WHERE event_logs.topic0 = X'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' AND event_logs.topic2 = ? AND event_logs.topic3 %v zeroblob(0) AND (block >= ? AND block <= ?)
        ORDER BY rowid %v LIMIT ? OFFSET ?
      )
    ORDER BY blocks.number %v, event_logs.logIndex %v`, topic3Comparison, topic3Comparison, sort, sort, sort),
    trimPrefix(addr.Bytes()), startBlock, endBlock, trimPrefix(addr.Bytes()), startBlock, endBlock, offset, (page - 1) * offset)
  if handleApiError(err, w, "database error", "Error! Database error", "Error processing", 500) { return }
  result := []*tokenTransfer{}
  for rows.Next() {
    var blockNumber uint64
    var blockTime, txNonce, txIndex,  txGas, txGasPrice, txCumulativeGasUsed, txGasUsed string
    var blockHash, tokenRecipient, txHash, tokenSender, tokenID, tokenValue, txInput, tokenContractAddress []byte
    err := rows.Scan(&blockNumber, &blockTime, &txHash, &txNonce, &blockHash, &tokenRecipient, &tokenSender, &tokenID, &tokenContractAddress, &tokenValue, &txIndex, &txGas, &txGasPrice, &txInput, &txCumulativeGasUsed, &txGasUsed)
    if handleApiError(err, w, "database error", "Error! Database error", "Error processing", 500) { return }
    token := chainTokens[bytesToAddress(tokenContractAddress)]
    item := &tokenTransfer{
      BlockNumber: fmt.Sprintf("%v", blockNumber),
      TimeStamp: blockTime,
      Hash: bytesToHash(txHash),
      Nonce: txNonce,
      BlockHash: bytesToHash(blockHash),
      From: bytesToAddress(tokenSender),
      ContractAddress: bytesToAddress(tokenContractAddress),
      To: bytesToAddress(tokenRecipient),
      TokenName: token.Name,
      TokenSymbol: token.Symbol,
      TokenDecimal: fmt.Sprintf("%v", token.Decimals),
      TransactionIndex: txIndex,
      Gas: txGas,
      GasPrice: txGasPrice,
      CumulativeGasUsed: txCumulativeGasUsed,
      GasUsed: txGasUsed,
      Confirmations: fmt.Sprintf("%v", (headBlockNumber - blockNumber) + 1),
    }
    if !nft {
      value, err := decompress(tokenValue)
      if handleApiError(err, w, "database error", "Error! Database error", "Error decompressing", 500) { return }
      item.Value = new(big.Int).SetBytes(value).String()
    } else {
      tokid := bytesToHash(tokenID)
      item.TokenID = &tokid
    }
    result = append(result, item)
  }
  if handleApiError(rows.Err(), w, "database error", "Error! Database error", "Error processing", 500) { return }
  handleApiResponse(w, 1, "OK", result, 200, len(result) == 0)
  return

}

type minersBlock struct {
  BlockNumber string `json:"blockNumber"`
  TimeStamp   string `json:"timeStamp"`
  BlockReward string `json:"blockReward"`
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
  sort := "DESC"
  if query.Get("sort") == "asc" {
    sort = "ASC"
  }
  if offset == 0 || offset > 10000 { offset = 10000 }
  var headBlockNumber uint64
  err := db.QueryRowContext(r.Context(), "SELECT max(number) FROM blocks;").Scan(&headBlockNumber)
  if headBlockNumber > uint64(endBlock) { endBlock = int(headBlockNumber) }
  // TODO: Use GROUP_CONCAT to avoid separate queries for gas usage
  rows, err := db.QueryContext(r.Context(),
    fmt.Sprintf(`SELECT
        blocks.number, blocks.time, issuance.value, GROUP_CONCAT(transactions.gasUsed), GROUP_CONCAT(transactions.gasPrice)
      FROM blocks
      INNER JOIN issuance on blocks.number > issuance.startBlock AND blocks.number < issuance.endBlock
      INNER JOIN transactions on transactions.block = blocks.number
      WHERE coinbase = ? AND (blocks.number >= ? AND blocks.number <= ?) GROUP BY blocks.number ORDER BY blocks.number %v LIMIT ? OFFSET ?;`, sort),
    trimPrefix(addr.Bytes()), startBlock, endBlock, offset, (page - 1) * offset)
  if handleApiError(err, w, "database error", "Error! Database error", "Error querying", 500) { return }
  result := []*minersBlock{}
  for rows.Next() {
    var blockNumber uint64
    var issuance int64
    var blockTime, gasUsedConcat, gasPriceConcat string
    if handleApiError(rows.Scan(&blockNumber, &blockTime, &issuance, &gasUsedConcat, &gasPriceConcat), w, "database error", "Error! Database error", "Error processing", 500) { return }
    gasUsedList := strings.Split(gasUsedConcat, ",")
    gasPriceList := strings.Split(gasPriceConcat, ",")
    reward := big.NewInt(issuance)
    for i := 0; i < len(gasUsedList); i++ {
      gasUsed, _ := new(big.Int).SetString(gasUsedList[i], 10)
      gasPrice, _ := new(big.Int).SetString(gasPriceList[i], 10)
      reward.Add(reward, new(big.Int).Mul(gasUsed, gasPrice))
    }
    result = append(result, &minersBlock{
      BlockNumber: fmt.Sprintf("%d", blockNumber),
      TimeStamp: blockTime,
      BlockReward: reward.String(),
    })
  }
  if handleApiError(rows.Err(), w, "database error", "Error! Database error", "Error processing", 500) { return }
  handleApiResponse(w, 1, "OK", result, 200, len(result) == 0)
}

type countdown struct {
  CurrentBlock string `json:"CurrentBlock"`
  CountdownBlock string `json:"CountdownBlock"`
  RemainingBlock string `json:"RemainingBlock"`
  EstimateTimeInSec string `json:"EstimateTimeInSec"`
}

func blockCountdown(w http.ResponseWriter, r *http.Request, db *sql.DB) {
  query := r.URL.Query()
  blockNo, _ := strconv.Atoi(query.Get("blockno"))
  var headBlockNumber int
  err := db.QueryRowContext(r.Context(), "SELECT max(number) FROM blocks;").Scan(&headBlockNumber)
  if handleApiError(err, w, "database error", "Error! Database error", "Error querying", 500) { return }
  if blockNo < headBlockNumber {
    handleApiResponse(w, 0, "NOTOK-missing", "Error! Block number already pass", 400, false)
    return
  }
  rows, err := db.QueryContext(r.Context(), `SELECT time FROM blocks ORDER BY number DESC LIMIT 100;`)
  if handleApiError(err, w, "database error", "Error! Database error", "Error querying", 500) { return }
  var lastBlock, cumulativeDifference, count int64
  if !rows.Next() {
    log.Printf("Error: No blocks available: %v",)
    handleApiResponse(w, 0, "NOTOK-database error", "Error! Database error", 500, false)
    return
  }
  if handleApiError(rows.Scan(&lastBlock), w, "database error", "Error! Database error", "Error scanning", 500) { return }
  for rows.Next() {
    var currentBlock int64
    if handleApiError(rows.Scan(&currentBlock), w, "database error", "Error! Database error", "Error scanning", 500) { return }
    cumulativeDifference += currentBlock - lastBlock
    count++
    lastBlock = currentBlock
  }
  if handleApiError(rows.Err(), w, "database error", "Error! Database error", "Error processing", 500) { return }
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
  operand := "<="
  value := "MAX(time)"
  if query.Get("closest") == "after" {
    operand = ">="
    value = "MIN(time)"
  }
  var blockNumber string
  log.Printf(fmt.Sprintf("SELECT %v, number FROM blocks WHERE time %v %v;", value, operand, timestamp))
  row := db.QueryRowContext(r.Context(), fmt.Sprintf("SELECT %v, number FROM blocks WHERE time %v ?;", value, operand), timestamp)
  if err := row.Scan(&timestamp, &blockNumber); err == sql.ErrNoRows {
    handleApiResponse(w, 0, "NOTOK-missing", "Error! No closest block found", 400, false)
    return
  } else if handleApiError(err, w, "database error", "Error! Database error", "Error selecting", 500) { return }
  handleApiResponse(w, 1, "OK", blockNumber, 200, false)
}


type tokenInfo struct {
  ContractAddress string `json:"contractAddress"`
  TokenName string `json:"tokenName"`
  Symbol string `json:"symbol"`
  Divisor json.Number `json:"divisor"`
  TokenType string `json:"tokenType"`
  TotalSupply string `json:"totalSupply"`
  BlueCheckmark string `json:"blueCheckmark"`
  Description string `json:"description"`
  Website string `json:"website"`
  Email string `json:"email"`
  Blog string `json:"blog"`
  Reddit string `json:"reddit"`
  Slack string `json:"slack"`
  Facebook string `json:"facebook"`
  Twitter string `json:"twitter"`
  Bitcointalk string `json:"bitcointalk"`
  Github string `json:"github"`
  Telegram string `json:"telegram"`
  Wechat string `json:"wechat"`
  Linkedin string `json:"linkedin"`
  Discord string `json:"discord"`
  Whitepaper string `json:"whitepaper"`
}

func getTokenInfo(w http.ResponseWriter, r *http.Request, db *sql.DB, chainTokens map[common.Address]tokens.Token) {
  // TODO: Query for total supply
  query := r.URL.Query()
  if query.Get("contractaddress") == "" {
    handleApiResponse(w, 0, "NOTOK-missing arguments", "Error! Missing account address", 400, false)
    return
  }
  addr := common.HexToAddress(query.Get("contractaddress"))
  token, ok := chainTokens[addr]
  if !ok {
    handleApiResponse(w, 0, "NOTOK-missing", "Error! Unknown token", 404, false)
    return
  }
  handleApiResponse(w, 1, "OK", tokenInfo{
    ContractAddress: addr.String(),
    TokenName: token.Name,
    Symbol: token.Symbol,
    Divisor: token.Decimals,
    TokenType: "ERC20",
    Website: token.Website,
    Email: token.Support.Email,
    Blog: token.Social["blog"],
    Reddit: token.Social["reddit"],
    Slack: token.Social["slack"],
    Facebook: token.Social["facebook"],
    Twitter: token.Social["twitter"],
    Github: token.Social["github"],
    Telegram: token.Social["telegram"],
    Wechat: token.Social["chat"],
    Linkedin: token.Social["linkedin"],
    Discord: token.Social["discord"],
  }, 200, false)
}
