package migrations

import (
  "database/sql"
  "log"
)

const (
  maxInt = 9223372036854775807
)

func Migrate(db *sql.DB, chainid uint64) error {
  var tableName string
  db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
  if tableName != "migrations" {
    db.Exec("CREATE TABLE migrations (version integer PRIMARY KEY);")
    db.Exec("INSERT INTO migrations(version) VALUES (0);")
  }
  var schemaVersion uint
  db.QueryRow("SELECT version FROM migrations;").Scan(&schemaVersion)
  if schemaVersion < 1 {
    if _, err := db.Exec(`CREATE TABLE blocks (
      number      BIGINT PRIMARY KEY,
      hash        varchar(32),
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
      td          varchar(32)
    );`); err != nil { return err }

    if _, err := db.Exec(`CREATE TABLE transactions (
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
      FOREIGN KEY(block) REFERENCES blocks(number) ON DELETE CASCADE
    );`); err != nil { return err }
    if _, err := db.Exec(`CREATE TABLE event_logs (
      address varchar(20),
      topic0 varchar(32),
      topic1 varchar(32),
      topic2 varchar(32),
      topic3 varchar(32),
      topic4 varchar(32),
      data blob,
      tx INTEGER,
      block BIGINT,
      logIndex MEDIUMINT,
      FOREIGN KEY(tx) REFERENCES transactions(id) ON DELETE CASCADE,
      FOREIGN KEY(block) REFERENCES blocks(number) ON DELETE CASCADE,
      PRIMARY KEY (tx, logIndex)
    );`); err != nil { return err }

    db.Exec(`CREATE INDEX address ON event_logs(address);`)
    db.Exec(`CREATE INDEX topic0 ON event_logs(topic0);`)
    db.Exec(`CREATE INDEX topic1 ON event_logs(topic1);`)
    db.Exec(`CREATE INDEX topic2 ON event_logs(topic2);`)
    db.Exec(`CREATE INDEX topic3 ON event_logs(topic3);`)
    db.Exec(`CREATE INDEX topic4 ON event_logs(topic4);`)
    db.Exec(`CREATE INDEX eventtx ON event_logs(tx);`)
    db.Exec(`CREATE INDEX eventblock ON event_logs(block);`)
    db.Exec(`CREATE INDEX txblock ON transactions(block);`)
    db.Exec(`CREATE INDEX hash ON blocks(hash);`)
    db.Exec(`CREATE INDEX coinbase ON blocks(coinbase);`)
    db.Exec(`CREATE INDEX timestamp ON blocks(time);`)
    db.Exec(`CREATE INDEX sender ON transactions(sender);`)
    db.Exec(`CREATE INDEX recipient ON transactions(recipient);`)
    db.Exec(`CREATE INDEX func ON transactions(func);`)


    db.Exec(`CREATE VIEW v_event_logs AS
      SELECT
        address,
        topic0,
        topic1,
        topic2,
        topic3,
        topic4,
        data,
        logIndex,
        block as blockNumber,
        transactions.transactionIndex as transactionIndex,
        transactions.hash as transactionHash,
        blocks.hash as blockHash
      FROM
        event_logs
      INNER JOIN transactions on transactions.id = event_logs.tx
      INNER JOIN blocks on blocks.number = event_logs.block;`)

    db.Exec(`CREATE VIEW v_transactions AS
      SELECT
        gas,
        gasPrice,
        transactions.hash as hash,
        input,
        transactions.nonce as nonce,
        recipient,
        transactionIndex,
        value,
        v,
        r,
        s,
        sender,
        func,
        contractAddress,
        cumulativeGasUsed,
        transactions.gasUsed as gasUsed,
        logsBloom,
        status,
        blocks as blockNumber,
        blocks.hash as blockHash
      FROM
        transactions
      INNER JOIN blocks on blocks.number = transactions.block;`)
    db.Exec(`CREATE TABLE offsets(offset BIGINT, topic varchar(32));`)
    db.Exec(`UPDATE migrations SET version = 1;`)
  }
  if schemaVersion < 2 {
    if _, err := db.Exec(`CREATE TABLE issuance (
      startBlock     BIGINT,
      endBlock       BIGINT,
      value          BIGINT
    );`); err != nil { return err }
    switch chainid {
    case 1:
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 1, 4369999, 5000000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 4370000, 7279999, 3000000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 7280000, maxInt, 2000000000000000000); err != nil { return err }
    case 61:
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 1, 5000000, 5000000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 5000001, 10000000, 4000000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 10000001, 15000000, 3200000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 15000001, 20000000, 2560000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 20000001, 25000000, 2048000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 25000001, maxInt, 0); err != nil { return err }
    case 3:
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 1, 1699999, 5000000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 1700000, 4229999, 3000000000000000000); err != nil { return err }
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 4230000, maxInt, 2000000000000000000); err != nil { return err }
    case 4:
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 1, maxInt, 0); err != nil { return err }
    case 5:
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 1, maxInt, 5000000000000000000); err != nil { return err }
    default:
      if _, err := db.Exec(`INSERT INTO issuance(startBlock, endBlock, value) VALUES (?, ?, ?)`, 1, maxInt, 0); err != nil { return err }
    }
    db.Exec(`UPDATE migrations SET version = 2;`)
  }
  if schemaVersion < 3 {
    db.Exec(`CREATE INDEX contractAddress ON transactions(contractAddress);`)
    db.Exec(`UPDATE migrations SET version = 3;`)
  }
  if schemaVersion < 4 {
    db.Exec(`ALTER TABLE transactions ADD type TINYINT;`)
    db.Exec(`ALTER TABLE transactions ADD access_list blob;`)
    db.Exec(`UPDATE migrations SET version = 4;`)
  }
  if schemaVersion < 5 {
    db.Exec(`DROP VIEW v_event_logs;`)
    db.Exec(`DROP VIEW v_transactions;`)
    db.Exec(`CREATE VIEW v_event_logs AS
      SELECT
        address,
        topic0,
        topic1,
        topic2,
        topic3,
        topic4,
        data,
        logIndex,
        event_logs.block as blockNumber,
        transactions.transactionIndex as transactionIndex,
        transactions.hash as transactionHash,
        blocks.hash as blockHash
      FROM
        event_logs
      INNER JOIN transactions on transactions.id = event_logs.tx
      INNER JOIN blocks on blocks.number = event_logs.block;`)
    db.Exec(`CREATE VIEW v_transactions AS
      SELECT
        gas,
        gasPrice,
        transactions.hash as hash,
        input,
        transactions.nonce as nonce,
        recipient,
        transactionIndex,
        value,
        v,
        r,
        s,
        sender,
        func,
        contractAddress,
        cumulativeGasUsed,
        transactions.gasUsed as gasUsed,
        logsBloom,
        status,
        transactions.block as blockNumber,
        blocks.hash as blockHash
      FROM
        transactions
      INNER JOIN blocks on blocks.number = transactions.block;`)
    db.Exec(`UPDATE migrations SET version = 5;`)
  }
  if schemaVersion < 6 {
    db.Exec(`ALTER TABLE event_logs ADD transactionHash varchar(32);`)
    db.Exec(`ALTER TABLE event_logs ADD transactionIndex varchar(32);`)
    db.Exec(`ALTER TABLE event_logs ADD blockHash varchar(32);`)
    var maxBlock, minBlock int
    db.QueryRow(`SELECT max(block) FROM event_logs;`).Scan(&maxBlock)
    db.QueryRow(`SELECT min(block) FROM event_logs WHERE transactionHash is null;`).Scan(&minBlock);
    if maxBlock == 0 {
      // Probably won't do anything, but if something's amiss this will pick anything up
      db.Exec(`UPDATE event_logs SET transactionHash = (SELECT hash from transactions WHERE id = event_logs.tx), transactionIndex = (SELECT transactionIndex from transactions WHERE id = event_logs.tx), blockHash = (SELECT hash from blocks WHERE number = event_logs.block);`)
    }
    for i := minBlock; i < maxBlock; i += 1000 {
      _, err := db.Exec(`UPDATE event_logs SET transactionHash = (SELECT hash from transactions WHERE id = event_logs.tx), transactionIndex = (SELECT transactionIndex from transactions WHERE id = event_logs.tx), blockHash = (SELECT hash from blocks WHERE number = event_logs.block) WHERE event_logs.block >= ? AND event_logs.block < ?;`, i, i + 1000)
      if err != nil { return err }
    }
    db.Exec(`DROP VIEW v_event_logs;`)
    db.Exec(`CREATE VIEW v_event_logs AS
      SELECT
        address,
        topic0,
        topic1,
        topic2,
        topic3,
        topic4,
        data,
        logIndex,
        event_logs.block as blockNumber,
        transactionIndex,
        transactionHash,
        blockHash
      FROM
        event_logs;`)
    db.Exec(`DROP INDEX eventtx;`)
    db.Exec(`UPDATE migrations SET version = 6;`)
  }
  if schemaVersion < 7 {
    db.Exec(`CREATE INDEX address_compound ON event_logs(address, block);`)
    db.Exec(`DROP INDEX address;`)
    db.Exec(`CREATE INDEX topic0_compound ON event_logs(topic0, block);`)
    db.Exec(`DROP INDEX topic0;`)
    db.Exec(`CREATE INDEX topic1_partial ON event_logs(topic1, block) WHERE topic1 IS NOT NULL;`)
    db.Exec(`DROP INDEX topic1;`)
    db.Exec(`CREATE INDEX topic2_partial ON event_logs(topic2, block) WHERE topic2 IS NOT NULL;`)
    db.Exec(`DROP INDEX topic2;`)
    db.Exec(`CREATE INDEX topic3_partial ON event_logs(topic3, block) WHERE topic3 IS NOT NULL;`)
    db.Exec(`DROP INDEX topic3;`)
    db.Exec(`CREATE INDEX topic4_partial ON event_logs(topic4, block) WHERE topic4 IS NOT NULL;`)
    db.Exec(`DROP INDEX topic4;`)
    db.Exec(`CREATE INDEX recipient_partial ON transactions(recipient) WHERE recipient IS NOT NULL;`)
    db.Exec(`DROP INDEX recipient;`)
    db.Exec(`CREATE INDEX func_partial ON transactions(func) WHERE func IS NOT NULL;`)
    db.Exec(`DROP INDEX func;`)
    db.Exec(`UPDATE migrations SET version = 7;`)
  }
  if schemaVersion < 8 {
    // topic4 doesn't exist - we can delete references to it
    db.Exec(`DROP INDEX topic4_partial;`)
    var maxBlock int
    db.QueryRow(`SELECT max(number) FROM blocks;`).Scan(&maxBlock)
    if maxBlock == 0 {
      // Probably won't do anything, but if something's amiss this will pick anything up
      db.Exec(`UPDATE transactions SET contractAddress = NULL WHERE contractAddress = X'00' OR contractAddress = zeroblob(0);`)
      db.Exec(`UPDATE transactions SET recipient = NULL WHERE recipient = zeroblob(0);`)
      db.Exec(`UPDATE transactions SET func = NULL WHERE func = zeroblob(0);`)
      db.Exec(`UPDATE event_logs SET topic0 = NULL WHERE topic0 = zeroblob(0);`)
      db.Exec(`UPDATE event_logs SET topic1 = NULL WHERE topic1 = zeroblob(0);`)
      db.Exec(`UPDATE event_logs SET topic2 = NULL WHERE topic2 = zeroblob(0);`)
      db.Exec(`UPDATE event_logs SET topic3 = NULL WHERE topic3 = zeroblob(0);`)
    }
    for i := 0; i < maxBlock; i += 1000 {
      // Handle ranges of blocks, as the go version of sqlite doesn't support update limits
      db.Exec(`UPDATE transactions SET contractAddress = NULL WHERE (contractAddress = X'00' OR contractAddress = zeroblob(0)) AND block >= ? AND block <= ?;`, i, i+1000)
      db.Exec(`UPDATE transactions INDEXED BY txblock SET recipient = NULL WHERE recipient = zeroblob(0) AND block >= ? AND block <= ?;`, i, i+1000)
      db.Exec(`UPDATE transactions INDEXED BY txblock SET func = NULL WHERE func = zeroblob(0) AND block >= ? AND block <= ?;`, i, i+1000)
      db.Exec(`UPDATE event_logs SET topic0 = NULL WHERE topic0 = zeroblob(0) AND block >= ? AND block <= ?;`, i, i+1000)
      db.Exec(`UPDATE event_logs SET topic1 = NULL WHERE topic1 = zeroblob(0) AND block >= ? AND block <= ?;`, i, i+1000)
      db.Exec(`UPDATE event_logs SET topic2 = NULL WHERE topic2 = zeroblob(0) AND block >= ? AND block <= ?;`, i, i+1000)
      db.Exec(`UPDATE event_logs SET topic3 = NULL WHERE topic3 = zeroblob(0) AND block >= ? AND block <= ?;`, i, i+1000)
      if i % 100000 == 0 {
        log.Printf("Migrating empty byte values to null. Block: %v / %v", i, maxBlock)
      }
    }
    db.Exec(`CREATE INDEX contractAddress_partial ON transactions(contractAddress) WHERE contractAddress IS NOT NULL;`)
    db.Exec(`DROP INDEX contractAddress;`)
    db.Exec(`UPDATE migrations SET version = 8;`)
  }
  if schemaVersion < 9 {
    db.Exec(`ALTER TABLE blocks ADD baseFee varchar(32);`)
    db.Exec(`ALTER TABLE transactions ADD gasFeeCap varchar(32);`)
    db.Exec(`ALTER TABLE transactions ADD gasTipCap varchar(32);`)
    db.Exec(`UPDATE migrations SET version = 9;`)
  }
	if schemaVersion < 10 {
		db.Exec(`CREATE INDEX senderNonce ON transactions(sender, nonce)`)
		db.Exec(`DROP INDEX sender`)
		db.Exec(`UPDATE migrations SET version = 10;`)
	}

	tableName = ""
	db.QueryRow("SELECT name FROM mempool.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE mempool.migrations (version integer PRIMARY KEY);")
		db.Exec("INSERT INTO mempool.migrations(version) VALUES (0);")
	}
	if err := db.QueryRow("SELECT version FROM mempool.migrations;").Scan(&schemaVersion); err != nil {
		return err
	}
	if schemaVersion < 1 {
		log.Printf("Applying mempool migration")
		if _, err := db.Exec(`CREATE TABLE mempool.transactions (
			gas BIGINT,
			gasPrice BIGINT,
			gasFeeCap varchar(32),
			gasTipCap varchar(32),
			hash varchar(32) UNIQUE,
			input blob,
			nonce BIGINT,
			recipient varchar(20),
			value varchar(32),
			v SMALLINT,
			r varchar(32),
			s varchar(32),
			sender varchar(20),
			type TINYINT,
			access_list blob
		);`); err != nil { return err }
		db.Exec(`CREATE INDEX sender ON mempool.transactions(sender, nonce);`)
		db.Exec(`CREATE INDEX recipient ON mempool.transactions(recipient);`)
		db.Exec(`CREATE INDEX gasPrice ON mempool.transactions(gasPrice);`)
		db.Exec(`UPDATE mempool.migrations SET version = 1;`)
	}
  // chainid
  return nil
}
