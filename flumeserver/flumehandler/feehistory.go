package flumehandler

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"database/sql"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"sort"
	"log"
)

type feeHistoryResult struct {
	OldestBlock  *hexutil.Big     `json:"oldestBlock"`
	Reward       [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee      []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio []float64        `json:"gasUsedRatio"`
}

// txGasAndReward is sorted in ascending order based on reward
type (
	txGasAndReward struct {
		gasUsed uint64
		reward  *big.Int
	}
	sortGasAndReward []txGasAndReward
)

func (s sortGasAndReward) Len() int { return len(s) }
func (s sortGasAndReward) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s sortGasAndReward) Less(i, j int) bool {
	return s[i].reward.Cmp(s[j].reward) < 0
}

func feeHistory(ctx context.Context, w http.ResponseWriter, call *rpcCall, db *sql.DB, chainid uint64) {
	var blockCount rpc.DecimalOrHex
	var lastBlock rpc.BlockNumber
	var rewardPercentiles []float64
	if len(call.Params) < 2 {
		handleError(w, fmt.Sprintf("expected at least 2 arguments"), call.ID, 400)
		return
	}
	if err := json.Unmarshal(call.Params[0], &blockCount); err != nil {
		handleError(w, fmt.Sprintf("error parsing argument 0: %v", err.Error()), call.ID, 400)
		return
	}
	if blockCount > 128 {
		blockCount = rpc.DecimalOrHex(128)
	} else if blockCount == 0 {
		blockCount = rpc.DecimalOrHex(20)
	}
	if err := json.Unmarshal(call.Params[1], &lastBlock); err != nil {
		handleError(w, fmt.Sprintf("error parsing argument 1: %v", err.Error()), call.ID, 400)
		return
	}
	log.Printf("lastblock: %v", lastBlock.Int64())
	if int64(lastBlock) < 0 {
		latestBlock, err := getLatestBlock(ctx, db)
		if err != nil {
			handleError(w, err.Error(), call.ID, 500)
			return
		}
		log.Printf("Setting lastBlock to latest")
		lastBlock = rpc.BlockNumber(latestBlock)
	}
	if len(call.Params) >= 3 {
		if err := json.Unmarshal(call.Params[2], &rewardPercentiles); err != nil {
			handleError(w, fmt.Sprintf("error parsing argument 2: %v", err.Error()), call.ID, 400)
			return
		}
	}
	rows, err := db.QueryContext(ctx, "SELECT baseFee, number, gasUsed, gasLimit FROM blocks WHERE number > ? LIMIT ?;", int64(lastBlock) - int64(blockCount), blockCount)
	if err != nil {
		handleError(w, err.Error(), call.ID, 500)
		return
	}
	result := &feeHistoryResult{
		OldestBlock:  (*hexutil.Big)(new(big.Int).SetInt64(int64(lastBlock) - int64(blockCount) + 1)),
		BaseFee:      make([]*hexutil.Big, int(blockCount) + 1),
		GasUsedRatio: make([]float64, int(blockCount)),
	}
	if len(rewardPercentiles) > 0 {
		result.Reward = make([][]*hexutil.Big, int(blockCount))
	}
	// TODO: Add next base fee to baseFeeList
	var lastBaseFee *big.Int
	var lastGasUsed, lastGasLimit int64
	for i := 0; rows.Next(); i++ {
		var baseFeeBytes []byte
		var number uint64
		var gasUsed, gasLimit sql.NullInt64
		if err := rows.Scan(&baseFeeBytes, &number, &gasUsed, &gasLimit); err != nil {
			handleError(w, fmt.Sprintf("error parsing block: %v", err.Error()), call.ID, 500)
			return
		}
		baseFee := new(big.Int).SetBytes(baseFeeBytes)
		lastBaseFee = baseFee
		result.BaseFee[i] = (*hexutil.Big)(baseFee)
		result.GasUsedRatio[i] = float64(gasUsed.Int64) / float64(gasLimit.Int64)
		lastGasUsed = gasUsed.Int64
		lastGasLimit = gasLimit.Int64
		if len(rewardPercentiles) > 0 {
			tips := sortGasAndReward{}
			txRows, err := db.QueryContext(ctx, "SELECT gasPrice, gasUsed FROM transactions WHERE block = ?;", number)
			if err != nil {
				handleError(w, fmt.Sprintf("error parsing transaction: %v", err.Error()), call.ID, 500)
				return
			}
			for txRows.Next() {
				var gasPrice, txGasUsed uint64
				if err := txRows.Scan(&gasPrice, &txGasUsed); err != nil {
					handleError(w, err.Error(), call.ID, 500)
					return
				}
				tip := new(big.Int).Sub(new(big.Int).SetUint64(gasPrice), baseFee)
				tips = append(tips, txGasAndReward{reward: tip, gasUsed: txGasUsed})
			}
			if err := txRows.Err(); err != nil {
				handleError(w, err.Error(), call.ID, 500)
				return
			}
			result.Reward[i] = make([]*hexutil.Big, len(rewardPercentiles))
			if len(tips) == 0 {
				for j := range rewardPercentiles {
					result.Reward[i][j] = new(hexutil.Big)
				}
				continue
			}
			sort.Sort(tips)
			var txIndex int
			sumGasUsed := tips[0].gasUsed
			for j, p := range rewardPercentiles {
				thresholdGasUsed := uint64(float64(gasUsed.Int64) * p / 100)
				for sumGasUsed < thresholdGasUsed && txIndex < len(tips) - 1 {
					txIndex++
					sumGasUsed += tips[txIndex].gasUsed
				}
				result.Reward[i][j] = (*hexutil.Big)(tips[txIndex].reward)
			}
		}
		if err := rows.Err(); err != nil {
			handleError(w, err.Error(), call.ID, 500)
			return
		}
	}

	gasTarget := lastGasLimit / 2
	if lastGasUsed == gasTarget {
		result.BaseFee[len(result.BaseFee) - 1] = (*hexutil.Big)(lastBaseFee)
	} else if lastGasUsed > gasTarget {
		delta := lastGasUsed - gasTarget
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(lastBaseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), big.NewInt(8))
		if baseFeeDelta.Cmp(new(big.Int)) == 0 {
			baseFeeDelta = big.NewInt(1)
		}
		result.BaseFee[len(result.BaseFee) - 1] = (*hexutil.Big)(new(big.Int).Add(lastBaseFee, baseFeeDelta))
	} else {
		delta := gasTarget - lastGasUsed
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(lastBaseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), big.NewInt(8))
		result.BaseFee[len(result.BaseFee) - 1] = (*hexutil.Big)(new(big.Int).Sub(lastBaseFee, baseFeeDelta))
	}
	responseBytes, err := json.Marshal(formatResponse(result, call))
	if err != nil {
		handleError(w, fmt.Sprintf("cannot marshal response: %v - %v", err.Error(), result), call.ID, 500)
		return
	}
	w.WriteHeader(200)
	w.Write(responseBytes)
}
