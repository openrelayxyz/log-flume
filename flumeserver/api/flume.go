package api

// import (
// 	"sort"
//   "bytes"
//   "github.com/klauspost/compress/zlib"
//   "strings"
//   "time"
//   "encoding/json"
//   "math/big"
//   "net/http"
//   "database/sql"
//   "github.com/ethereum/go-ethereum/common"
//   "github.com/ethereum/go-ethereum/common/hexutil"
//   "github.com/ethereum/go-ethereum/core/types"
//   "github.com/ethereum/go-ethereum/eth/filters"
//   "github.com/ethereum/go-ethereum/rlp"
//   "github.com/ethereum/go-ethereum/rpc"
//   "io/ioutil"
//   "io"
//   "context"
//   "fmt"
//   "log"
//   "sync"
// )
//
// type FlumeAPI struct {
// 	db *sql.DB
// 	network uint64
// }
//
// func NewFlumeAPI (db *sql.DB, network uint64 ) *LogsAPI {
// 	return &LogsAPI{
// 		db: db,
// 		network: network,
// 	}
// }
//
// func (api *FlumeAPI) Flume() string {
// 	return "goodbuy horses"
// }
