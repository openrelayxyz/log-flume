package migrations

import (
  "database/sql"
)


func Migrate(db *sql.DB) error {
  var tableName string
  db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
  if tableName != "migrations" {
    db.Exec("CREATE TABLE migrations (version integer PRIMARY KEY);")
    db.Exec("INSERT INTO migrations(version) VALUES (0);")
  }
  var schemaVersion uint
  db.QueryRow("SELECT version FROM migrations;").Scan(&schemaVersion)
  if schemaVersion == 0 {
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
        blocks.number as blockNumber,
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
        blocks.number as blockNumber,
        blocks.hash as blockHash
      FROM
        transactions
      INNER JOIN blocks on blocks.number = transactions.block;`)
    db.Exec(`CREATE TABLE offsets(offset BIGINT, topic varchar(32));`)
    db.Exec(`UPDATE migrations SET version = 1;`)
  }
  return nil
}
