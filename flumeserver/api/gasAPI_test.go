package api

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"compress/gzip"
	"encoding/json"
	"github.com/ethereum/go-ethereum/rpc"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
)

func feeDataDecompress() (map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("fee_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var feeData map[string]json.RawMessage
	json.Unmarshal(raw, &feeData)
	return feeData, nil
}

func getRewardsList(jsonObject json.RawMessage) []json.RawMessage {
	var result []json.RawMessage
	json.Unmarshal(jsonObject, &result)
	return result
}

func TestGasAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	g := NewGasAPI(db, 1)

	price := "0x2a51edbe67"
	fee := "0x9502f900"

	t.Run(fmt.Sprintf("GasPrice"), func(t *testing.T) {
		actual, _ := g.GasPrice(context.Background())
		if actual != price {
			t.Fatalf("GasPrice error")
		}
	})
	t.Run(fmt.Sprintf("MaxPriorityFeePerGas"), func(t *testing.T) {
		actual, _ := g.MaxPriorityFeePerGas(context.Background())
		if actual != fee {
			t.Fatalf("MaxPriorityFeePerGas error")
		}
	})

	feeData, _ := feeDataDecompress()
	t.Run(fmt.Sprintf("FeeHistory"), func(t *testing.T) {
		var blockCount rpc.DecimalOrHex = 0x15
		var lastBlock rpc.BlockNumber = 0xd59f95
		percentiles := []float64{.1, .5, .9}

		actual, _ := g.FeeHistory(context.Background(), blockCount, lastBlock, percentiles)
		oldestBlockData, err := json.Marshal(actual.OldestBlock)
		if err != nil {
			t.Errorf(err.Error())
		}
		if !bytes.Equal(oldestBlockData, feeData["oldestBlock"]) {
			t.Fatalf("FeeHistory oldestBlock Error")
		}
		var outerSlice []json.RawMessage
		json.Unmarshal(feeData["reward"], &outerSlice)
		for i, slice := range actual.Reward {
			var innerSlice []json.RawMessage
			json.Unmarshal(outerSlice[i], &innerSlice)
			for j, value := range slice {
				rewardData, err := json.Marshal(value)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(rewardData, innerSlice[j]) {
					t.Fatalf("FeeHistory reward Error on %v %v", i, j)
				}
			}
		}
		var baseFeeSlice []json.RawMessage
		json.Unmarshal(feeData["baseFeePerGas"], &baseFeeSlice)
		for i, fee := range actual.BaseFee {
			data, err := json.Marshal(fee)
			if err != nil {
				t.Errorf(err.Error())
			}
			if !bytes.Equal(data, baseFeeSlice[i]) {
				t.Fatalf("FeeHistory BaseFeePerGas Error on index%v", i)
			}
		}
		var gasUsedSlice []json.RawMessage
		json.Unmarshal(feeData["gasUsedRatio"], &gasUsedSlice)
		for i, ratio := range actual.GasUsedRatio {
			data, err := json.Marshal(ratio)
			if err != nil {
				t.Errorf(err.Error())
			}
			if !bytes.Equal(data, gasUsedSlice[i]) {
				t.Fatalf("FeeHistory GasUsedRatio Error on index%v", i)
			}
		}
	})
}
