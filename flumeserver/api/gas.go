package api

import (
	"sort"
  // "bytes"
  // "github.com/klauspost/compress/zlib"
  // "strings"
  // "time"
  // "encoding/json"
  "math/big"
  // "net/http"
  "database/sql"
  // "github.com/ethereum/go-ethereum/common"
  // "github.com/ethereum/go-ethereum/common/hexutil"
  // "github.com/ethereum/go-ethereum/core/types"
  // "github.com/ethereum/go-ethereum/eth/filters"
  // "github.com/ethereum/go-ethereum/rlp"
  // "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  "context"
  // "fmt"
  // "log"
  // "sync"
)

type GasAPI struct {
	db *sql.DB
	network uint64
}

func NewGasAPI (db *sql.DB, network uint64 ) *LogsAPI {
	return &LogsAPI{
		db: db,
		network: network,
	}
}

func (api *GasAPI) Gas() string {
	return "goodbuy horses"
}

// case "eth_gasPrice":
// 	gasPrice(r.Context(), w, call, db, chainid)
// case "eth_feeHistory":
// 	feeHistory(r.Context(), w, call, db, chainid)
// case "eth_maxPriorityFeePerGas":
// 	maxPriorityFeePerGas(r.Context(), w, call, db, chainid)

func (api *GasAPI) gasTip(ctx context.Context) (*big.Int, error) {
  latestBlock, err := getLatestBlock(ctx, api.db)
  if err != nil {
    return nil, err
  }
  rows, err := api.db.QueryContext(ctx, "SELECT gasPrice, baseFee from transactions INNER JOIN blocks ON transactions.block = blocks.number WHERE blocks.number > ?;", latestBlock - 20)
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
	err = api.db.QueryRowContext(ctx, "SELECT gasPrice, baseFee from transactions INNER JOIN blocks ON transactions.block = blocks.number WHERE 1 ORDER BY id DESC LIMIT 1;").Scan(&gasPrice, &baseFeeBytes)
	return new(big.Int).Sub(big.NewInt(gasPrice), new(big.Int).SetBytes(baseFeeBytes)), err
}

func (api *GasAPI) nextBaseFee(ctx context.Context) (*big.Int, error) {
	var baseFeeBytes []byte
	var gasLimit, gasUsed int64
	err := api.db.QueryRowContext(ctx, "SELECT baseFee, gasUsed, gasLimit FROM blocks ORDER BY number DESC LIMIT 1;").Scan(&baseFeeBytes, &gasUsed, &gasLimit)
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

func (api *GasAPI) GasPrice(ctx context.Context) (*big.Int, error) {
	tip, err := api.gasTip(ctx)
	if err != nil {
		return nil, err
	}
	baseFee, err := api.nextBaseFee(ctx)
	if err != nil {
		return nil, err
	}
	return 
}
