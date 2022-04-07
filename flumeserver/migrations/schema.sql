CREATE TABLE blocks.cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition))
CREATE TABLE blocks.issuance (
startBlock     BIGINT,
endBlock       BIGINT,
value          BIGINT
)
CREATE TABLE blocks.migrations (version integer PRIMARY KEY)
CREATE TABLE blocks.blocks (
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
      baseFee varchar(32))
CREATE TABLE logs.event_logs (
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
    )
CREATE TABLE logs.migrations (version integer PRIMARY KEY)
CREATE TABLE tx.migrations (version integer PRIMARY KEY)
CREATE TABLE tx.transactions (
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
    )

CREATE INDEX eventblock ON logs.event_logs(block)
CREATE INDEX txblock ON tx.transactions(block)
CREATE INDEX coinbase ON blocks.blocks(coinbase)
CREATE INDEX timestamp ON blocks.blocks(time)
CREATE INDEX address_compound ON logs.event_logs(address, block)
CREATE INDEX topic0_compound ON logs.event_logs(topic0, block)
CREATE INDEX topic1_partial ON logs.event_logs(topic1, block) WHERE topic1 IS NOT NULL
CREATE INDEX topic2_partial ON logs.event_logs(topic2, block) WHERE topic2 IS NOT NULL
CREATE INDEX topic3_partial ON logs.event_logs(topic3, block) WHERE topic3 IS NOT NULL
CREATE INDEX recipient_partial ON tx.transactions(recipient) WHERE recipient IS NOT NULL
CREATE INDEX contractAddress_partial ON tx.transactions(contractAddress) WHERE contractAddress IS NOT NULL
CREATE INDEX senderNonce ON tx.transactions(sender, nonce)
