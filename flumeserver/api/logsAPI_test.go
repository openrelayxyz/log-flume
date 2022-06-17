package api

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/inconshreveable/log15"
)

func TestLogsAPI(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := connectToDatabase()
	if err != nil {
		log.Error("LogsAPI test failure", "failed to load logsDB", err.Error())
	}
	defer db.Close()
	l := NewLogsAPI(db, 1)

	t.Run(fmt.Sprintf("Testing GetLogs BlockHash"), func(t *testing.T) {
		hashes := []common.Hash{}
		row, err := db.Query("SELECT DISTINCT HEX(blockHash) from event_logs;")
		if err != nil {
			log.Error("LogsAPI blockHash failure", "failed to query database", err.Error())
		}
		defer row.Close()
		for row.Next() {
			var hash string
			row.Scan(&hash)
			item := "0x" + hash
			hashes = append(hashes, common.HexToHash(item))
		}
		hash := hashes[rand.Intn(len(hashes))]
		arg := FilterQuery{
			BlockHash: &hash,
		}
		actual, err := l.GetLogs(context.Background(), arg)
		if err != nil {
			t.Errorf("GetLogs returned an error %v", arg)
		}
		if len(actual) == 0 {
			t.Errorf("Empty list")
		}
		for _, item := range actual {
			if item.BlockHash != hash {
				t.Fatalf("logs hash error %v", item)
			}
		}
	})

	t.Run(fmt.Sprintf("Testing GetLogs Address"), func(t *testing.T) {
		addresses := []common.Address{}
		row, err := db.Query("SELECT DISTINCT HEX(address) from event_logs WHERE block >= 14000000 AND block <= 14000021;")
		if err != nil {
			log.Error("LogsAPI address failure", "failed to query database", err.Error())
		}
		defer row.Close()
		for row.Next() {
			var address string
			row.Scan(&address)
			item := "0x" + address
			if len(item) == 42 {
				addresses = append(addresses, common.HexToAddress(item))
			}
		}
		fb := big.NewInt(14000000)
		lb := big.NewInt(14000021)
		address := addresses[rand.Intn(len(addresses))]
		arg := FilterQuery{
			FromBlock: fb,
			ToBlock:   lb,
			Addresses: []common.Address{address},
		}
		actual, err := l.GetLogs(context.Background(), arg)
		if err != nil {
			t.Errorf("GetLogs returned an error %v", arg)
		}
		if len(actual) == 0 {
			t.Errorf("Empty list, Address: %v", address)
		}
		for _, item := range actual {
			if item.Address != address {
				t.Fatalf("logs address error %v", item)
			}
		}
	})
	t.Run(fmt.Sprintf("Testing GetLogs Topic0"), func(t *testing.T) {
		topicZeroes := []common.Hash{}
		row, err := db.Query("SELECT DISTINCT HEX(topic0) from event_logs WHERE block >= 14000000 AND block <= 14000021;")
		if err != nil {
			log.Error("LogsAPI topic0 failure", "failed to query database", err.Error())
		}
		defer row.Close()
		for row.Next() {
			var topic0 string
			row.Scan(&topic0)
			item := "0x" + topic0
			if len(item) == 66 {
				topicZeroes = append(topicZeroes, common.HexToHash(item))
			}
		}
		fb := big.NewInt(14000000)
		lb := big.NewInt(14000021)
		topic0 := topicZeroes[rand.Intn(len(topicZeroes))]
		topicList := []common.Hash{topic0}
		arg := FilterQuery{
			FromBlock: fb,
			ToBlock:   lb,
			Topics:    [][]common.Hash{topicList},
		}
		actual, err := l.GetLogs(context.Background(), arg)
		if err != nil {
			t.Errorf("GetLogs returned an error %v", arg)
		}
		if len(actual) == 0 {
			t.Errorf("Empty list, Topic0: %v", topic0)
		}
		for _, item := range actual {
			for _, data := range item.Topics {
				if len(item.Topics) > 1 {
					if item.Topics[0] != topic0 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, len(item.Topics), item.Topics, topic0)
					}
				} else {
					if data != topic0 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, item.Topics, data, topic0)
					}
				}
			}
		}
	})
	t.Run(fmt.Sprintf("Testing GetLogs Topic1"), func(t *testing.T) {
		topicOnes := []common.Hash{}
		row, err := db.Query("SELECT DISTINCT HEX(topic1) from event_logs WHERE block >= 14000000 AND block <= 14000021;")
		if err != nil {
			log.Error("LogsAPI topic1 failure", "failed to query database", err.Error())
		}
		defer row.Close()
		for row.Next() {
			var topic1 string
			row.Scan(&topic1)
			item := "0x" + topic1
			if len(item) == 66 {
				topicOnes = append(topicOnes, common.HexToHash(item))
			}
		}
		fb := big.NewInt(14000000)
		lb := big.NewInt(14000021)
		topic1 := topicOnes[rand.Intn(len(topicOnes))]
		topicList := []common.Hash{topic1}

		arg := FilterQuery{
			FromBlock: fb,
			ToBlock:   lb,
			Topics:    [][]common.Hash{[]common.Hash{}, topicList},
		}
		actual, err := l.GetLogs(context.Background(), arg)
		if err != nil {
			t.Errorf("GetLogs returned an error %v", arg)
		}
		if len(actual) == 0 {
			t.Errorf("Empty list, Topic1: %v", topic1)
		}
		for _, item := range actual {
			for _, data := range item.Topics {
				if len(item.Topics) > 1 {
					if item.Topics[1] != topic1 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, len(item.Topics), item.Topics, topic1)
					}
				} else {
					if data != topic1 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, len(item.Topics), item.Topics, topic1)
					}
				}
			}
		}
	})
	t.Run(fmt.Sprintf("Testing GetLogs Topic2"), func(t *testing.T) {
		topicTwos := []common.Hash{}
		row, err := db.Query("SELECT DISTINCT HEX(topic2) from event_logs WHERE block >= 14000000 AND block <= 14000021;")
		if err != nil {
			log.Error("LogsAPI topic2 failure", "failed to query database", err.Error())
		}
		defer row.Close()
		for row.Next() {
			var topic2 string
			row.Scan(&topic2)
			item := "0x" + topic2
			if len(item) == 66 {
				topicTwos = append(topicTwos, common.HexToHash(item))
			}
		}
		fb := big.NewInt(14000000)
		lb := big.NewInt(14000021)
		topic2 := topicTwos[rand.Intn(len(topicTwos))]
		topicList := []common.Hash{topic2}
		arg := FilterQuery{
			FromBlock: fb,
			ToBlock:   lb,
			Topics:    [][]common.Hash{[]common.Hash{}, []common.Hash{}, topicList},
		}
		actual, err := l.GetLogs(context.Background(), arg)
		if err != nil {
			t.Errorf("GetLogs returned an error %v", arg)
		}
		if len(actual) == 0 {
			t.Errorf("Empty list, Topic2: %v", topic2)
		}
		for _, item := range actual {
			for _, data := range item.Topics {
				if len(item.Topics) > 1 {
					if item.Topics[2] != topic2 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, len(item.Topics), item.Topics, topic2)
					}
				} else {
					if data != topic2 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, len(item.Topics), item.Topics, topic2)
					}
				}
			}
		}
	})
	t.Run(fmt.Sprintf("Testing GetLogs Topic3"), func(t *testing.T) {
		topicThrees := []common.Hash{}
		row, err := db.Query("SELECT DISTINCT HEX(topic3) from event_logs WHERE block >= 14000000 AND block <= 14000021;")
		if err != nil {
			log.Error("LogsAPI topic3 failure", "failed to query database", err.Error())
		}
		defer row.Close()
		for row.Next() {
			var topic3 string
			row.Scan(&topic3)
			item := "0x" + topic3
			if len(item) == 66 {
				topicThrees = append(topicThrees, common.HexToHash(item))
			}
		}
		fb := big.NewInt(14000000)
		lb := big.NewInt(14000021)
		topic3 := topicThrees[rand.Intn(len(topicThrees))]
		topicList := []common.Hash{topic3}
		arg := FilterQuery{
			FromBlock: fb,
			ToBlock:   lb,
			Topics:    [][]common.Hash{[]common.Hash{}, []common.Hash{}, []common.Hash{}, topicList},
		}
		actual, err := l.GetLogs(context.Background(), arg)
		if err != nil {
			t.Errorf("GetLogs returned an error %v", arg)
		}
		if len(actual) == 0 {
			t.Errorf("Empty list, Topic3: %v", topic3)
		}
		for _, item := range actual {
			for _, data := range item.Topics {
				if len(item.Topics) > 1 {
					if item.Topics[3] != topic3 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, len(item.Topics), item.Topics, topic3)
					}
				} else {
					if data != topic3 {
						t.Errorf("topic error %v %v %v %v", arg.Topics, len(item.Topics), item.Topics, topic3)
					}
				}
			}
		}
	})
}
