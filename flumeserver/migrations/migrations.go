package migrations

import (
	"database/sql"
	"log"
)

const (
	maxInt = 9223372036854775807
)

func MigrateBlocks(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM blocks.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName);

	if tableName != "migrations" {
		db.Exec("CREATE TABLE blocks.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO blocks.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM blocks.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {

		db.Exec(`CREATE TABLE blocks.blocks (
		      number      BIGINT PRIMARY KEY,
		      hash        varchar(32) UNIQUE,
		      parentHash  varchar(32),
		      uncleHash   varchar(32),
		      coinbase    varchar(20),
		      root        varchar(32),
		      txRoot      varchar(32),
		      receiptRoot varchar(32),
		      bloom       blob,
		      difficulty  varchar(32),
		      gasLimit    BIGINT,
		      gasUsed     BIGINT,
		      time        BIGINT,
		      extra       blob,
		      mixDigest   varchar(32),
		      nonce       BIGINT,
		      uncles      blob,
		      size        BIGINT,
		      td          varchar(32),
		      baseFee varchar(32))`)

		if _, err := db.Exec(`CREATE TABLE blocks.cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition))`); err != nil {
			log.Printf(err.Error())
		}
		if _, err := db.Exec(`CREATE TABLE blocks.issuance (
					startBlock     BIGINT,
					endBlock       BIGINT,
					value          BIGINT
					)`); err != nil {
						log.Printf(err.Error())
					}

		if _, err := db.Exec(`CREATE INDEX blocks.coinbase ON blocks(coinbase)`); err != nil { log.Printf(err.Error()) }
		if _, err := db.Exec(`CREATE INDEX blocks.timestamp ON blocks(time)`); err != nil { log.Printf(err.Error()) }
		db.Exec(`UPDATE blocks.migrations SET version = 1;`)

	}
	return nil
}

func MigrateTransactions(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM transactions.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE transactions.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO transactions.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM transactions.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		db.Exec(`CREATE TABLE transactions.transactions (
		      id INTEGER PRIMARY KEY AUTOINCREMENT,
		      gas BIGINT,
		      gasPrice BIGINT,
		      hash varchar(32) UNIQUE,
		      input blob,
		      nonce BIGINT,
		      recipient varchar(20),
		      transactionIndex MEDIUMINT,
		      value varchar(32),
		      v SMALLINT,
		      r varchar(32),
		      s varchar(32),
		      sender varchar(20),
		      func varchar(4),
		      contractAddress varchar(20),
		      cumulativeGasUsed BIGINT,
		      gasUsed BIGINT,
		      logsBloom blob,
		      status TINYINT,
		      block BIGINT,
		      type TINYINT,
		      access_list blob,
		      gasFeeCap varchar(32),
		      gasTipCap varchar(32),
		    )`)
		if _, err := db.Exec(`CREATE INDEX transactions.txblock ON transactions(block)`); err != nil { log.Printf(err.Error()) }
		if _, err := db.Exec(`CREATE INDEX transactions.recipient_partial ON transactions(recipient) WHERE recipient IS NOT NULL`); err != nil { log.Printf(err.Error()) }
		if _, err := db.Exec(`CREATE INDEX transactions.contractAddress_partial ON transactions(contractAddress) WHERE contractAddress IS NOT NULL`); err != nil { log.Printf(err.Error()) }
		if _, err := db.Exec(`CREATE INDEX transactions.senderNonce ON transactions(sender, nonce)`); err != nil { log.Printf(err.Error()) }
		db.Exec(`UPDATE transactions.migrations SET version = 1;`)

	}
	return nil
}

func MigrateLogs(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM logs.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE logs.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO logs.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM logs.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		db.Exec(`CREATE TABLE logs.event_logs (
		      address varchar(20),
		      topic0 varchar(32),
		      topic1 varchar(32),
		      topic2 varchar(32),
		      topic3 varchar(32),
		      data blob,
		      block BIGINT,
		      logIndex MEDIUMINT,
		      transactionHash varchar(32),
		      transactionIndex varchar(32),
		      blockHash varchar(32),
		      PRIMARY KEY (transactionHash)
		    )`)
		if _, err := db.Exec(`CREATE INDEX logs.address_compound ON event_logs(address, block)`); err != nil {
			panic(err)
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic0_compound ON event_logs(topic0, block)`); err != nil { log.Printf(err.Error()) }
		if _, err := db.Exec(`CREATE INDEX logs.topic1_partial ON event_logs(topic1, block) WHERE topic1 IS NOT NULL`); err != nil { log.Printf(err.Error()) }
		if _, err := db.Exec(`CREATE INDEX logs.topic2_partial ON event_logs(topic2, block) WHERE topic2 IS NOT NULL`); err != nil { log.Printf(err.Error()) }
		if _, err := db.Exec(`CREATE INDEX logs.topic3_partial ON event_logs(topic3, block) WHERE topic3 IS NOT NULL`); err != nil { log.Printf(err.Error()) }
		db.Exec(`UPDATE logs.migrations SET version = 1;`)

	}
	return nil
}
