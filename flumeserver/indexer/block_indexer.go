package indexer

import (
	// "encoding/binary"
	"fmt"
	"io"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	// "github.com/openrelayxyz/cardinal-evm/crypto"
  "github.com/ethereum/go-ethereum/crypto"
	// "github.com/openrelayxyz/cardinal-evm/types"
  "github.com/ethereum/go-ethereum/core/types"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	ctypes "github.com/openrelayxyz/cardinal-types"
	"golang.org/x/crypto/sha3"
	"math/big"
	"regexp"
	"sync"
	"strconv"
)

var (
	uncleRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/u/([0-9a-z]+)")
	txRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/t/([0-9a-z]+)")
)

type rlpData []byte

func (d rlpData) EncodeRLP(w io.Writer) error {
	_, err := w.Write(d)
	return err
}

var hasherPool = sync.Pool{
	New: func() interface{} { return sha3.NewLegacyKeccak256() },
}

// rlpHash encodes x and hashes the encoded bytes.
func hash(x []byte) (ctypes.Hash) {
	sha := hasherPool.Get().(crypto.KeccakState)
	defer hasherPool.Put(sha)
	sha.Reset()
	return ctypes.BytesToHash(sha.Sum(x))
}

type BlockIndexer struct {
	chainid uint64
}

type extblock struct {
	Header *types.Header
	Txs    []types.Transaction
	Uncles []rlpData
}

func NewBlockIndexer(chainid uint64) Indexer {
	return &BlockIndexer{chainid: chainid}
}

func (indexer *BlockIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {
	headerBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/h", indexer.chainid, pb.Hash.Bytes())]
	tdBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/d", indexer.chainid, pb.Hash.Bytes())]
	td := new(big.Int).SetBytes(tdBytes)
	header := &types.Header{}
	if err := rlp.DecodeBytes(headerBytes, &header); err != nil {
		panic(err.Error())
	}

	eblock := &extblock{
		Header: header,
		Txs: []types.Transaction{},
		Uncles: []rlpData{},
	}

	uncleHashes := make(map[int64]ctypes.Hash)
	txData := make(map[int64]types.Transaction)
	uncleData := make(map[int64]rlpData)

	for k, v := range pb.Values {
		switch {
		case uncleRegexp.MatchString(k):
			parts := uncleRegexp.FindSubmatch([]byte(k))
			uncleIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			uncleHashes[int64(uncleIndex)] = hash(v)
			uncleData[int64(uncleIndex)] = rlpData(v)
		case txRegexp.MatchString(k):
			parts := txRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			var tx types.Transaction
			tx.UnmarshalBinary(v)
			txData[int64(txIndex)] = tx
		default:
		}
	}
	eblock.Txs = make([]types.Transaction, len(txData))
	for i, v := range txData {
		eblock.Txs[int(i)] = v
	}
	eblock.Uncles = make([]rlpData, len(uncleData))
	for i, v := range uncleData {
		eblock.Uncles[int(i)] = v
	}
	ebd, _ := rlp.EncodeToBytes(eblock)
	size := len(ebd)
	uncles := make([]ctypes.Hash, len(uncleHashes))
	for i, v := range uncleHashes {
		uncles[int(i)] = v
	}
	uncleRLP, _ := rlp.EncodeToBytes(uncles)
	statements := []string{
		applyParameters("DELETE FROM blocks WHERE number >= %v", pb.Number),
	}
	statements = append(statements, applyParameters(
		"INSERT INTO blocks(number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, `time`, extra, mixDigest, nonce, uncles, size, td, baseFee) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
		pb.Number,
		pb.Hash,
		pb.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		compress(header.Bloom[:]),
		header.Difficulty.Int64(),
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra,
		header.MixDigest,
		header.Nonce, //type mis-allignment means that we may not get accurate statements from .dumps on old test data.
		//Nonce was appearing twice, which do we want to keep?
		// binary.BigEndian.Uint64(header.Nonce[:]),
		uncleRLP,
		size,
		td.Bytes(),
		header.BaseFee,
	))
	return statements, nil
}
