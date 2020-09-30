package migrations

import (
  "database/sql"
)


func Migrate(db *sql.DB) {
  var tableName string
  db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
  if tableName != "migrations" {
    db.Exec("CREATE TABLE migrations (version integer PRIMARY KEY);")
    db.Exec("INSERT INTO migrations(version) VALUES (0);")
  }
  var schemaVersion uint
  db.QueryRow("SELECT version FROM migrations;").Scan(&schemaVersion)
  if schemaVersion == 0 {
    db.Exec(`CREATE TABLE transactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      blockHash varchar(32),
      blockNumber BIGINT,
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
      status TINYINT
    );`)

    db.Exec(`CREATE TABLE event_logs (
      address varchar(20),
      topic0 varchar(32),
      topic1 varchar(32),
      topic2 varchar(32),
      topic3 varchar(32),
      topic4 varchar(32),
      data blob,
      tx INTEGER,
      logIndex MEDIUMINT,
      FOREIGN KEY(tx) REFERENCES transactions(id) ON DELETE CASCADE,
      PRIMARY KEY (tx, logIndex)
    );`)

    db.Exec(`CREATE INDEX address ON event_logs(address);`)
    db.Exec(`CREATE INDEX topic0 ON event_logs(topic0);`)
    db.Exec(`CREATE INDEX topic1 ON event_logs(topic1);`)
    db.Exec(`CREATE INDEX topic2 ON event_logs(topic2);`)
    db.Exec(`CREATE INDEX topic3 ON event_logs(topic3);`)
    db.Exec(`CREATE INDEX topic4 ON event_logs(topic4);`)
    db.Exec(`CREATE INDEX blockNumber ON transactions(blockNumber);`)
    db.Exec(`CREATE INDEX blockHash ON transactions(blockHash);`)
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
        transactions.blockNumber as blockNumber,
        transactions.transactionIndex as transactionIndex,
        transactions.hash as transactionHash,
        transactions.blockHash as blockHash
      FROM
        event_logs
      INNER JOIN transactions on transactions.id = event_logs.tx;`)
    db.Exec(`CREATE TABLE offsets(offset BIGINT, topic varchar(32));`)
    db.Exec(`UPDATE migrations SET version = 1;`)
  }
}
