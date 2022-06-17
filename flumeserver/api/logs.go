package api

import (
	"database/sql"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"sort"
	"strings"
	"context"
	"fmt"
	log "github.com/inconshreveable/log15"
)

type LogsAPI struct {
	db      *sql.DB
	network uint64
}

func NewLogsAPI(db *sql.DB, network uint64) *LogsAPI {
	return &LogsAPI{
		db:      db,
		network: network,
	}
}

func (api *LogsAPI) GetLogs(ctx context.Context, crit FilterQuery) ([]*types.Log, error) {
	latestBlock, err := getLatestBlock(ctx, api.db)
	if err != nil {
		// handleError(err.Error(), call.ID, 500)
		return nil, err
	}

	whereClause := []string{}
	indexClause := ""
	params := []interface{}{}
	if crit.BlockHash != nil {
		var num int64
		api.db.QueryRowContext(ctx, "SELECT number FROM blocks WHERE hash = ?", crit.BlockHash.Bytes()).Scan(&num)
		whereClause = append(whereClause, "blockHash = ? AND block = ?")
		params = append(params, trimPrefix(crit.BlockHash.Bytes()), num)
	} else {
		var fromBlock, toBlock int64
		if crit.FromBlock == nil || crit.FromBlock.Int64() < 0 {
			fromBlock = latestBlock
		} else {
			fromBlock = crit.FromBlock.Int64()
		}
		whereClause = append(whereClause, "block >= ?")
		params = append(params, fromBlock)
		if crit.ToBlock == nil || crit.ToBlock.Int64() < 0 {
			toBlock = latestBlock
		} else {
			toBlock = crit.ToBlock.Int64()
		}
		whereClause = append(whereClause, "block <= ?")
		params = append(params, toBlock)
	}
	addressClause := []string{}
	for _, address := range crit.Addresses {
		addressClause = append(addressClause, "address = ?")
		params = append(params, trimPrefix(address.Bytes()))
	}
	if len(addressClause) > 0 {
		whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(addressClause, " OR ")))
	}
	topicsClause := []string{}
	for i, topics := range crit.Topics {
		topicClause := []string{}
		for _, topic := range topics {
			topicClause = append(topicClause, fmt.Sprintf("topic%v = ?", i))
			params = append(params, trimPrefix(topic.Bytes()))
		}
		if len(topicClause) > 0 {
			topicsClause = append(topicsClause, fmt.Sprintf("(%v)", strings.Join(topicClause, " OR ")))
		} else {
			topicsClause = append(topicsClause, fmt.Sprintf("topic%v IS NOT NULL", i))
		}
	}
	if len(topicsClause) > 0 {
		whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(topicsClause, " AND ")))
	}
	query := fmt.Sprintf("SELECT address, topic0, topic1, topic2, topic3, data, block, transactionHash, transactionIndex, blockHash, logIndex FROM event_logs %v WHERE %v;", indexClause, strings.Join(whereClause, " AND "))
	rows, err := api.db.QueryContext(ctx, query, params...)
	if err != nil {
		log.Error("Error selecting query", "query", query, "err", err.Error())
		return nil, err
	}
	defer rows.Close()
	logs := sortLogs{}
	blockNumbersInResponse := make(map[uint64]struct{})
	for rows.Next() {
		var address, topic0, topic1, topic2, topic3, data, transactionHash, blockHash []byte
		var blockNumber uint64
		var transactionIndex, logIndex uint
		err := rows.Scan(&address, &topic0, &topic1, &topic2, &topic3, &data, &blockNumber, &transactionHash, &transactionIndex, &blockHash, &logIndex)
		if err != nil {
			log.Error("Error scanning", "err", err.Error())
			// handleError("database error", call.ID, 500)
			return nil, fmt.Errorf("database error")
		}
		blockNumbersInResponse[blockNumber] = struct{}{}
		topics := []common.Hash{}
		if len(topic0) > 0 {
			topics = append(topics, bytesToHash(topic0))
		}
		if len(topic1) > 0 {
			topics = append(topics, bytesToHash(topic1))
		}
		if len(topic2) > 0 {
			topics = append(topics, bytesToHash(topic2))
		}
		if len(topic3) > 0 {
			topics = append(topics, bytesToHash(topic3))
		}
		input, err := decompress(data)
		if err != nil {
			log.Error("Error decompressing data", "err", err.Error())
			// handleError("database error", call.ID, 500)
			return nil, fmt.Errorf("database error")
		}
		logs = append(logs, &types.Log{
			Address:     bytesToAddress(address),
			Topics:      topics,
			Data:        input,
			BlockNumber: blockNumber,
			TxHash:      bytesToHash(transactionHash),
			TxIndex:     transactionIndex,
			BlockHash:   bytesToHash(blockHash),
			Index:       logIndex,
		})
		if len(logs) > 10000 && len(blockNumbersInResponse) > 1 {
			// handleError("query returned more than 10,000 results spanning multiple blocks", call.ID, 413)
			return nil, fmt.Errorf("query returned more than 10,000 results spanning multiple blocks")
		}
	}
	if err := rows.Err(); err != nil {
		log.Error("Error scanning", "err", err.Error())
		// handleError("database error", call.ID, 500)
		return nil, fmt.Errorf("database error")
	}
	sort.Sort(logs)

	return logs, nil
}
