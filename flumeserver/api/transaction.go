package api

import (
	"context"
	"database/sql"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

type TransactionAPI struct {
	db      *sql.DB
	network uint64
}

func NewTransactionAPI(db *sql.DB, network uint64) *TransactionAPI {
	return &TransactionAPI{
		db:      db,
		network: network,
	}
}

func (api *TransactionAPI) GetTransactionByHash(ctx context.Context, txHash common.Hash) (map[string]interface{}, error) {
	var err error
	offset := new(int)
	*offset = 0
	txs, err := getTransactionsBlock(ctx, api.db, *offset, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	if err != nil {
		return nil, err
	}
	if len(txs) == 0 {
		txs, err = getPendingTransactions(ctx, api.db, *offset, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	}
	if err != nil {
		return nil, err
	}

	result := returnSingleTransaction(txs)

	return result, nil
}

func (api *TransactionAPI) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint64) (map[string]interface{}, error) {
	var err error
	var offset *int
	v := 0
	offset = &v
	txs, err := getTransactionsBlock(ctx, api.db, *offset, 1, api.network, "blocks.hash = ? AND transactionIndex = ?", trimPrefix(blockHash.Bytes()), uint64(index))
	if err != nil {
		return nil, err
	}
	result := returnSingleTransaction(txs)

	return result, nil
}

func (api *TransactionAPI) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index hexutil.Uint64) (map[string]interface{}, error) {
	var offset *int
	v := 0
	offset = &v
	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}

	txs, err := getTransactionsBlock(ctx, api.db, *offset, 1, api.network, "block = ? AND transactionIndex = ?", uint64(blockNumber), uint64(index))
	if err != nil {
		return nil, err
	}

	result := returnSingleTransaction(txs)

	return result, nil
}

func (api *TransactionAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (map[string]interface{}, error) {
	var err error
	var offset *int
	v := 0
	offset = &v
	receipts, err := getTransactionReceiptsBlock(ctx, api.db, *offset, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	if err != nil {
		return nil, err
	}
	result := returnSingleReceipt(receipts)

	return result, nil
}

func (api *TransactionAPI) GetTransactionCount(ctx context.Context, addr common.Address) (hexutil.Uint64, error) {
	nonce, err := getSenderNonce(ctx, api.db, addr)
	if err != nil {
		return 0, err
	}

	return nonce, nil
}
