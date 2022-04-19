package api

import (
	"database/sql"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	eh "github.com/openrelayxyz/flume/flumeserver/errhandle"
	"math/big"
	"sort"

	"context"
)

type GasAPI struct {
	db      *sql.DB
	network uint64
}

func NewGasAPI(db *sql.DB, network uint64) *GasAPI {
	return &GasAPI{
		db:      db,
		network: network,
	}
}

func (api *GasAPI) gasTip(ctx context.Context) (*big.Int, error) {
	latestBlock, err := getLatestBlock(ctx, api.db)
	if err != nil {
		return nil, err
	}
	rows, err := api.db.QueryContext(ctx, "SELECT gasPrice, baseFee from transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number WHERE blocks.number > ?;", latestBlock-20)
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
			tips = append(tips, tip)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(tips) > 0 {
		sort.Sort(tips)
		return tips[(len(tips)*6)/10], nil
	}
	var gasPrice int64
	var baseFeeBytes []byte
	err = api.db.QueryRowContext(ctx, "SELECT gasPrice, baseFee from transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number WHERE 1 ORDER BY id DESC LIMIT 1;").Scan(&gasPrice, &baseFeeBytes)
	return new(big.Int).Sub(big.NewInt(gasPrice), new(big.Int).SetBytes(baseFeeBytes)), err
}

func (api *GasAPI) nextBaseFee(ctx context.Context) (*big.Int, error) {
	var baseFeeBytes []byte
	var gasLimit, gasUsed int64
	err := api.db.QueryRowContext(ctx, "SELECT baseFee, gasUsed, gasLimit FROM blocks.blocks ORDER BY blocks.number DESC LIMIT 1;").Scan(&baseFeeBytes, &gasUsed, &gasLimit)
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

func (api *GasAPI) GasPrice(ctx context.Context) (string, error) {
	var err error
	tip, err := api.gasTip(ctx)
	if err != nil {
		return "", err
	}
	baseFee, err := api.nextBaseFee(ctx)
	if err != nil {
		return "", err
	}
	sum := big.NewInt(0)
	sum.Add(tip, baseFee)
	result := hexutil.EncodeBig(sum)
	return result, nil
}

func (api *GasAPI) MaxPriorityFeePerGas(ctx context.Context) (res string, err error) {
	defer eh.HandleErr(&err)
	return hexutil.EncodeBig(eh.CheckAndAssign(api.gasTip(ctx))), nil
}

func (api *GasAPI) FeeHistory(ctx context.Context, blockCount rpc.DecimalOrHex, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (res *feeHistoryResult, err error) {
	defer eh.HandleErr(&err)

	if blockCount > 128 {
		blockCount = rpc.DecimalOrHex(128)
	} else if blockCount == 0 {
		blockCount = rpc.DecimalOrHex(20)
	}

	if int64(lastBlock) < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		lastBlock = rpc.BlockNumber(latestBlock)
	}

	rows := eh.CheckAndAssign(api.db.QueryContext(ctx, "SELECT baseFee, number, gasUsed, gasLimit FROM blocks.blocks WHERE number > ? LIMIT ?;", int64(lastBlock)-int64(blockCount), blockCount))

	result := &feeHistoryResult{
		OldestBlock:  (*hexutil.Big)(new(big.Int).SetInt64(int64(lastBlock) - int64(blockCount) + 1)),
		BaseFee:      make([]*hexutil.Big, int(blockCount)+1),
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
		eh.Check(rows.Scan(&baseFeeBytes, &number, &gasUsed, &gasLimit))
		baseFee := new(big.Int).SetBytes(baseFeeBytes)
		lastBaseFee = baseFee
		result.BaseFee[i] = (*hexutil.Big)(baseFee)
		result.GasUsedRatio[i] = float64(gasUsed.Int64) / float64(gasLimit.Int64)
		lastGasUsed = gasUsed.Int64
		lastGasLimit = gasLimit.Int64
		if len(rewardPercentiles) > 0 {
			tips := sortGasAndReward{}
			txRows := eh.CheckAndAssign(api.db.QueryContext(ctx, "SELECT gasPrice, gasUsed FROM transactions.transactions WHERE block = ?;", number))
			for txRows.Next() {
				var gasPrice, txGasUsed uint64
				eh.Check(txRows.Scan(&gasPrice, &txGasUsed))
				tip := new(big.Int).Sub(new(big.Int).SetUint64(gasPrice), baseFee)
				tips = append(tips, txGasAndReward{reward: tip, gasUsed: txGasUsed})
			}
			eh.Check(txRows.Err())
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
				for sumGasUsed < thresholdGasUsed && txIndex < len(tips)-1 {
					txIndex++
					sumGasUsed += tips[txIndex].gasUsed
				}
				result.Reward[i][j] = (*hexutil.Big)(tips[txIndex].reward)
			}
		}
		eh.Check(rows.Err())
	}

	gasTarget := lastGasLimit / 2
	if lastGasUsed == gasTarget {
		result.BaseFee[len(result.BaseFee)-1] = (*hexutil.Big)(lastBaseFee)
	} else if lastGasUsed > gasTarget {
		delta := lastGasUsed - gasTarget
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(lastBaseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), big.NewInt(8))
		if baseFeeDelta.Cmp(new(big.Int)) == 0 {
			baseFeeDelta = big.NewInt(1)
		}
		result.BaseFee[len(result.BaseFee)-1] = (*hexutil.Big)(new(big.Int).Add(lastBaseFee, baseFeeDelta))
	} else {
		delta := gasTarget - lastGasUsed
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(lastBaseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), big.NewInt(8))
		result.BaseFee[len(result.BaseFee)-1] = (*hexutil.Big)(new(big.Int).Sub(lastBaseFee, baseFeeDelta))
	}
	return result, nil
}
