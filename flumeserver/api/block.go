package api

import (
	"context"
	"database/sql"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

type BlockAPI struct {
	db      *sql.DB
	network uint64
}

func NewBlockAPI(db *sql.DB, network uint64) *BlockAPI {
	return &BlockAPI{
		db:      db,
		network: network,
	}
}

func (api *BlockAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	blockNo, err := getLatestBlock(ctx, api.db)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(blockNo), nil
}

func (api *BlockAPI) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, includeTxns bool) (map[string]interface{}, error) {

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}

	blocks, err := getBlocks(ctx, api.db, includeTxns, api.network, "number = ?", blockNumber)
	if err != nil {
		return nil, err
	}
	var blockVal map[string]interface{}
	if len(blocks) > 0 {
		blockVal = blocks[0]
	}
	return blockVal, nil
}

func (api *BlockAPI) GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (map[string]interface{}, error) {

	blocks, err := getBlocks(ctx, api.db, includeTxs, api.network, "hash = ?", trimPrefix(blockHash.Bytes()))
	if err != nil {
		return nil, err
	}
	var blockVal map[string]interface{}
	if len(blocks) > 0 {
		blockVal = blocks[0]
	}
	return blockVal, nil
}

func (api *BlockAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	var err error
	var count hexutil.Uint64

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}
	count, err = txCount(ctx, api.db, "block = ?", blockNumber)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (api *BlockAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (hexutil.Uint64, error) {
	var count hexutil.Uint64
	block, err := getBlocks(ctx, api.db, false, api.network, "hash = ?", trimPrefix(blockHash.Bytes()))
	if err != nil {
		return 0, err
	}
	var blockVal map[string]interface{}
	if len(block) > 0 {
		blockVal = block[0]
	}
	blockNumber := int64(blockVal["number"].(hexutil.Uint64))

	count, err = txCount(ctx, api.db, "block = ?", rpc.BlockNumber(blockNumber))
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (api *BlockAPI) GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	var uncles []byte
	unclesList := []common.Hash{}

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}
	if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE number = ?", blockNumber).Scan(&uncles); err != nil {
		return 0, err
	}
	rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil

}

func (api *BlockAPI) GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (hexutil.Uint64, error) {
	var uncles []byte
	unclesList := []common.Hash{}

	if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE hash = ?", trimPrefix(blockHash.Bytes())).Scan(&uncles); err != nil {
		return 0, err
	}
	rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil
}
