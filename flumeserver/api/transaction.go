package api

// import (
// 	"sort"
//   // "bytes"
//   // "github.com/klauspost/compress/zlib"
//   "strings"
//   // "time"
//   // "encoding/json"
//   // "math/big"
//   // "net/http"
//   "database/sql"
//   "github.com/ethereum/go-ethereum/common"
//   // "github.com/ethereum/go-ethereum/common/hexutil"
//   "github.com/ethereum/go-ethereum/core/types"
//   "github.com/ethereum/go-ethereum/eth/filters"
//   // "github.com/ethereum/go-ethereum/rlp"
//   // "github.com/ethereum/go-ethereum/rpc"
//   // "io/ioutil"
//   // "io"
//   "context"
//   "fmt"
//   "log"
//   // "sync"
// )
//
// type TransactionAPI struct {
// 	db *sql.DB
// 	network uint64
// }
//
// func NewTransactionAPI (db *sql.DB, network uint64 ) *LogsAPI {
// 	return &LogsAPI{
// 		db: db,
// 		network: network,
// 	}
// }
//
//
// func (api *TransactionsAPI) Transactions() string {
// 	return "goodbuy horses"
// }
//
// // case "eth_getTransactionByHash":
// // 	getTransactionByHash(r.Context(), w, call, db, chainid)
// // case "eth_getTransactionByBlockHashAndIndex":
// // 	getTransactionByBlockHashAndIndex(r.Context(), w, call, db, chainid)
// // case "eth_getTransactionByBlockNumberAndIndex":
// // 	getTransactionByBlockNumberAndIndex(r.Context(), w, call, db, chainid)
// // case "eth_getTransactionReceipt":
// // 	getTransactionReceipt(r.Context(), w, call, db, chainid)
// // case "eth_getBlockTransactionCountByNumber":
// // 	getBlockTransactionCountByNumber(r.Context(), w, call, db, chainid)
// // case "eth_getBlockTransactionCountByHash":
// // 	getBlockTransactionCountByHash(r.Context(), w, call, db, chainid)
// // case "eth_getTransactionCount":
// // 	getTransactionCount(r.Context(), w, call, db, chainid)
