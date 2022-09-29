package datafeed

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
    "encoding/json"
	"github.com/ethereum/go-ethereum/event"
	gtypes "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	ctypes "github.com/openrelayxyz/cardinal-types"
	"log"
	"math/big"
	"sync/atomic"
	"strings"
	"strconv"
)

var (
	txRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/t/([0-9a-z]+)")
	uncleRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/u/([0-9a-z]+)")
	receiptRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/r/([0-9a-z]+)")
	logRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/l/([0-9a-z]+)/([0-9a-z]+)")
)

type cardinalDataFeed struct{
	consumer transports.Consumer
	sub      ctypes.Subscription
	ready    chan struct{}
	lastBlockTime *atomic.Value
  feed event.Feed
  started bool
	chainid int64
}

func NewCardinalDataFeed(brokerURL string, rollback, reorgThreshold, chainid, resumptionTime int64, db *sql.DB, whitelist map[uint64]ctypes.Hash) (DataFeed, error) {
	if whitelist == nil { whitelist = make(map[uint64]ctypes.Hash) }
	var tableName string
	db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='cardinal_offsets';").Scan(&tableName)
	if tableName != "cardinal_offsets" {
		if _, err := db.Exec("CREATE TABLE cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition));"); err != nil {
			return nil, fmt.Errorf("Offsets table does not exist and could not create: %v", err.Error())
		}
	}
	parts := strings.Split(brokerURL, ";")
	topics := strings.Split(parts[1], ",")
	startOffsets := []string{}
	for _, topic := range topics {
		var partition int32
		var offset int64
		rows, err := db.QueryContext(context.Background(), "SELECT partition, offset FROM cardinal_offsets WHERE topic = ?;", topic)
		if err != nil { continue }
		for rows.Next() {
			if err := rows.Scan(&partition, &offset); err != nil { return nil, err }
			startOffsets = append(startOffsets, fmt.Sprintf("%v:%v=%v", topic, partition, offset))
		}
	}
	resumption := strings.Join(startOffsets, ";")
	var lastHash, lastWeight []byte
	var lastNumber, n int64
	db.QueryRowContext(context.Background(), "SELECT max(number) FROM blocks;").Scan(&n)
	db.QueryRowContext(context.Background(), "SELECT number, hash, td FROM blocks WHERE number = ?;", n - 3).Scan(&lastNumber, &lastHash, &lastWeight)

	trackedPrefixes := []*regexp.Regexp{
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/h"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/d"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/u"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/t/"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/r/"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/l/"),
	}
	var consumer transports.Consumer
	var err error
	log.Printf("Resuming to block number %v", lastNumber)
	if strings.HasPrefix(parts[0], "kafka://") {
		rt := []byte(resumption)
		if resumptionTime > 0 {
			r, err := transports.ResumptionForTimestamp([]transports.BrokerParams{
				{URL: brokerURL, Topics: topics},
				}, resumptionTime)
			if err != nil {
				log.Printf("Could not load resumption from timestamp: %v", err.Error())
			} else {
				rt = r
			}
		}
		consumer, err = transports.NewKafkaConsumer(parts[0], topics[0], topics, rt, rollback, lastNumber, ctypes.BytesToHash(lastHash), new(big.Int).SetBytes(lastWeight), reorgThreshold, trackedPrefixes, whitelist)
	} else if strings.HasPrefix(parts[0], "null://") {
		consumer = transports.NewNullConsumer()
	}
	if err != nil { return nil, err }
	return &cardinalDataFeed{
		consumer: consumer,
		ready: make(chan struct{}),
		lastBlockTime: &atomic.Value{},
		chainid: chainid,
	}, nil
}

func (d *cardinalDataFeed) subscribe() {
	if d.sub != nil  {
		log.Printf("data feed already started")
		return
	}
	ch := make(chan *delivery.ChainUpdate)
	d.sub = d.consumer.Subscribe(ch)
	go func() {
		for {
			select {
			case err := <-d.sub.Err():
				log.Printf("Subscription closing: %v", err.Error())
				return
			case update := <-ch:
				for i, pb := range update.Added() {
					ce := d.chainEventFromCardinalBatch(pb)
					if i < len(update.Added()) - 1 {
						// If there are multiple chain events in a group, we only want to
						// commit offsets to the database for the last item in the group, as
						// these same offsets triggered the emit of all of the events.
						ce.Commit = func(*sql.Tx) (error) { return nil }
					} else {
						ce.Commit = func(tx *sql.Tx) (error) {
							if tx == nil { return nil }
							resumption := pb.Resumption()
							tokens := strings.Split(resumption, ";")
							for _, token := range tokens {
								parts := strings.Split(token, "=")
								source, offsetS := parts[0], parts[1]
								parts = strings.Split(source, ":")
								topic, partitionS := parts[0], parts[1]
								offset, err := strconv.Atoi(offsetS)
								if err != nil {
									log.Printf("error parsing offset", err.Error())
									continue
								}
								partition, err := strconv.Atoi(partitionS)
								if err != nil {
									log.Printf("error parsing partition", err.Error())
									continue
								}
								if _, err := tx.Exec("INSERT OR REPLACE INTO cardinal_offsets(offset, partition, topic) VALUES (?, ?, ?)", offset, partition, topic); err != nil {
									return err
								}
							}
							return nil
						}
					}
					d.feed.Send(ce)
					pb.Done()
				}
			}
		}
	}()
	if err := d.consumer.Start(); err != nil {
		panic(err.Error())
	}
}

func (d *cardinalDataFeed) Close() {
	d.sub.Unsubscribe()
	d.consumer.Close()
}
func (d *cardinalDataFeed) Subscribe(ch chan *ChainEvent) event.Subscription {
	sub := d.feed.Subscribe(ch)
	if !d.started {
		d.subscribe()
		d.started = true
	}
	return sub
}
func (d *cardinalDataFeed) Ready() <-chan struct{} {
	return d.consumer.Ready()
}

type txBundle struct {
	tx *Transaction
	rm *cardinalReceiptMeta
	logs map[int]*gtypes.Log
}

type cardinalReceiptMeta struct {
	ContractAddress common.Address
	CumulativeGasUsed uint64
	GasUsed uint64
	LogsBloom []byte
	Status uint64
	LogCount uint
	LogOffset uint
}

type extblock struct {
	Header *gtypes.Header
	Txs    []*Transaction
	Uncles []*gtypes.Header
}

// chainEventFromCardinalBatch converts a
// cardinal-streams/delivery.PendingBatch into a chain event consumable by
// Flume.
func (d *cardinalDataFeed) chainEventFromCardinalBatch(pb *delivery.PendingBatch) *ChainEvent {
	headerBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/h", d.chainid, pb.Hash.Bytes())]
	tdBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/d", d.chainid, pb.Hash.Bytes())]
	td := new(big.Int).SetBytes(tdBytes)
	header := &gtypes.Header{}
	if err := rlp.DecodeBytes(headerBytes, &header); err != nil {
		panic(err.Error())
	}
	uncles := make(map[int]*gtypes.Header)
	ce := &ChainEvent{
		Block: &miniBlock{
			Difficulty: *(*hexutil.Big)(header.Difficulty),
			ExtraData: hexutil.Bytes(header.Extra),
			GasLimit: hexutil.Uint64(header.GasLimit),
			GasUsed: hexutil.Uint64(header.GasUsed),
			Hash: common.Hash(pb.Hash),
			LogsBloom: hexutil.Bytes(header.Bloom[:]),
			Coinbase: common.Address(header.Coinbase),
			MixHash: common.Hash(header.MixDigest),
			Nonce: header.Nonce,
			Number: hexutil.Big(*header.Number),
			ParentHash: common.Hash(header.ParentHash),
			ReceiptRoot: common.Hash(header.ReceiptHash),
			Sha3Uncles: common.Hash(header.UncleHash),
			// Size: hexutil.Uint64(kce.Block.Size()), // TODO: We'll need to figure out how to calculate this
			StateRoot: common.Hash(header.Root),
			Timestamp: hexutil.Uint64(header.Time),
			TotalDifficulty: hexutil.Big(*td),
			// Transactions: []types.Transaction{},
			TransactionsRoot: common.Hash(header.TxHash),
			// Uncles: uncles,
			BaseFee: (*hexutil.Big)(header.BaseFee),
		},
		logs: make(map[common.Hash][]*gtypes.Log),
		receiptMeta: make(map[common.Hash]*receiptMeta),
	}
	eb := &extblock{
		Header: header,
	}
	txBundles := make(map[int]*txBundle)
	logCount := 0
	for k, v := range pb.Values {
		switch {
		case txRegexp.MatchString(k):
			parts := txRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			if _, ok := txBundles[int(txIndex)]; !ok {
				txBundles[int(txIndex)] = &txBundle{logs: make(map[int]*gtypes.Log)}
			}
			tx := &Transaction{}
            json.Unmarshal(v, &tx)
			txBundles[int(txIndex)].tx = tx
		case uncleRegexp.MatchString(k):
			parts := uncleRegexp.FindSubmatch([]byte(k))
			uncleIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			h := &gtypes.Header{}
			rlp.DecodeBytes(v, h)
			uncles[int(uncleIndex)] = h
		case receiptRegexp.MatchString(k):
			parts := receiptRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			if _, ok := txBundles[int(txIndex)]; !ok {
				txBundles[int(txIndex)] = &txBundle{logs: make(map[int]*gtypes.Log)}
			}
			rmeta := &cardinalReceiptMeta{}
			rlp.DecodeBytes(v, rmeta)
			txBundles[int(txIndex)].rm = rmeta
		case logRegexp.MatchString(k):
			parts := logRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			logIndex, _ := strconv.ParseInt(string(parts[3]), 16, 64)
			if _, ok := txBundles[int(txIndex)]; !ok {
				txBundles[int(txIndex)] = &txBundle{logs: make(map[int]*gtypes.Log)}
			}
			logRecord := &gtypes.Log{}
			rlp.DecodeBytes(v, logRecord)
			logRecord.BlockNumber = uint64(pb.Number)
			logRecord.TxIndex = uint(txIndex)
			logRecord.BlockHash = common.Hash(pb.Hash)
			logRecord.Index = uint(logIndex)
			txBundles[int(txIndex)].logs[int(logIndex)] = logRecord
			logCount++
		}
	}
	ce.Block.Uncles = make([]common.Hash, len(uncles))
	eb.Uncles = make([]*gtypes.Header, len(uncles))
	for i, uncle := range uncles {
		ce.Block.Uncles[i] = uncle.Hash()
		eb.Uncles[i] = uncle
	}
	eb.Txs = make([]*Transaction, len(txBundles))
	ce.Block.Transactions = make([]*Transaction, len(txBundles))
	ce.logs = make(map[common.Hash][]*gtypes.Log)
	for i, v := range txBundles {
		txHash := v.tx.Hash()
		eb.Txs[i] = v.tx
		ce.Block.Transactions[i] = v.tx
		ce.receiptMeta[txHash] = &receiptMeta{
			contractAddress: v.rm.ContractAddress,
			cumulativeGasUsed: v.rm.CumulativeGasUsed,
			gasUsed: v.rm.GasUsed,
			logsBloom: gtypes.BytesToBloom(v.rm.LogsBloom),
			status: v.rm.Status,
		}
		ce.logs[txHash] = make([]*gtypes.Log, len(v.logs))
		for i, logRecord := range v.logs {
			logRecord.TxHash = txHash
			ce.logs[txHash][i - int(v.rm.LogOffset)] = logRecord
		}
	}
	ebd, _ := rlp.EncodeToBytes(eb)
	ce.Block.Size = hexutil.Uint64(len(ebd))
	return ce
}
