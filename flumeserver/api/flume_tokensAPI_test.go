package api

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"compress/gzip"
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
)

func tokenDataDecompress() ([][]common.Address, error) {
	file, _ := ioutil.ReadFile("token_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var tokenData [][]common.Address
	json.Unmarshal(raw, &tokenData)
	return tokenData, nil
}

func TestERCMethods(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	ft := NewFlumeTokensAPI(db, 1)

	data, _ := tokenDataDecompress()

	address := "0xdac17f958d2ee523a2206206994597c13d831ec7"

	t.Run(fmt.Sprintf("GetERC20Holders"), func(t *testing.T) {
		actual, _ := ft.GetERC20Holders(context.Background(), common.HexToAddress(address), 0)
		for i, addr := range actual.Items {
			if addr != data[0][i] {
				t.Fatalf("GetERC20Holders error")
			}
		}
	})
	t.Run(fmt.Sprintf("GetERC20ByAccount"), func(t *testing.T) {
		actual, _ := ft.GetERC20ByAccount(context.Background(), common.HexToAddress(address), 0)
		for i, addr := range actual.Items {
			if addr != data[1][i] {
				t.Fatalf("GetERC20ByAccount error")
			}
		}
	})
}
