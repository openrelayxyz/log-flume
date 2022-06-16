package main

import (
	"context"
	"database/sql"
	"fmt"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/transports"
	streamsTransports "github.com/openrelayxyz/cardinal-streams/transports"
	ctypes "github.com/openrelayxyz/cardinal-types"
	"math/big"
	"regexp"
	"strings"
)

func AquireConsumer(db *sql.DB, brokerParams []transports.BrokerParams, reorgThreshold, chainid, resumptionTime int64) (streamsTransports.Consumer, error) {
	var err error
	var tableName string
	db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='cardinal_offsets';").Scan(&tableName)
	if tableName != "cardinal_offsets" {
		if _, err = db.Exec("CREATE TABLE cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition));"); err != nil {
			return nil, err
		}
	}
	startOffsets := []string{}
	for _, broker := range brokerParams {
		for _, topic := range broker.Topics {
			var partition int32
			var offset int64
			rows, err := db.QueryContext(context.Background(), "SELECT partition, offset FROM cardinal_offsets WHERE topic = ?;", topic)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				if err := rows.Scan(&partition, &offset); err != nil {
					return nil, err
				}
				startOffsets = append(startOffsets, fmt.Sprintf("%v:%v=%v", topic, partition, offset))
			}
		}
	}
	resumption := strings.Join(startOffsets, ";")
	var lastHash, lastWeight []byte
	var lastNumber int64
	db.QueryRowContext(context.Background(), "SELECT max(number), hash, td FROM blocks;").Scan(&lastNumber, &lastHash, &lastWeight)

	trackedPrefixes := []*regexp.Regexp{
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/h"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/d"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/u"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/t/"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/r/"),
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/l/"),
	}
	log.Info("Resuming to block", "number", lastNumber)
	rt := []byte(resumption)
	if resumptionTime > 0 {
		r, err := streamsTransports.ResumptionForTimestamp(brokerParams, resumptionTime)
		if err != nil {
			log.Warn("Could not load resumption from timestamp:", "error", err.Error())
		} else {
			rt = r
		}
	}
	return streamsTransports.ResolveMuxConsumer(brokerParams, rt, lastNumber, ctypes.BytesToHash(lastHash), new(big.Int).SetBytes(lastWeight), reorgThreshold, trackedPrefixes, nil)
}
