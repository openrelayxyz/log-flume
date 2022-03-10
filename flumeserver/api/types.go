package api

import (
  // "sort"
  // "bytes"
  // "github.com/klauspost/compress/zlib"
  // "strings"
  // "time"
  // "encoding/json"
  "math/big"
  // "net/http"
  // "database/sql"
  // "github.com/ethereum/go-ethereum/common"
  // "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/core/types"
  // "github.com/ethereum/go-ethereum/eth/filters"
  // "github.com/ethereum/go-ethereum/rlp"
  // "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  // "context"
  // "fmt"
  // "log"
  // "sync"
)

type bigList []*big.Int

func (ms bigList) Len() int {
	return len(ms)
}

func (ms bigList) Less(i, j int) bool {
	return ms[i].Cmp(ms[j]) < 0
}

func (ms bigList) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

type sortLogs []*types.Log


func (ms sortLogs) Len() int {
  return len(ms)
}

func (ms sortLogs) Less(i, j int) bool {
  if ms[i].BlockNumber != ms[j].BlockNumber {
    return ms[i].BlockNumber < ms[j].BlockNumber
  }
  return ms[i].Index < ms[j].Index
}

func (ms sortLogs) Swap(i, j int) {
  ms[i], ms[j] = ms[j], ms[i]
}
