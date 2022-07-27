package datafeed


import (
  "context"
  "strings"
  "fmt"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/rawdb"
  "path"
  "log"
)

func ResolveFeed(url string, db *sql.DB, kafkaRollback, finishedLimit int64, chainid uint64, timestamp int64) (DataFeed, error) {
  if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") || strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://")  {
    return NewETHWSFeed(url, db)
  } else if strings.HasPrefix(url, "file://") {
    parts := strings.Split(strings.TrimPrefix(url, "file://"), ";")
    dbpath := parts[0]
    ancients := path.Join(parts[0], "ancient")
    if len(parts) >= 2 {
      ancients = parts[1]
    }
    ldb, err := rawdb.NewLevelDBDatabaseWithFreezer(dbpath, 16, 16, ancients, "dbfeed", true)
    if err != nil { return nil, err }
    var resumeBlock int64
    db.QueryRowContext(context.Background(), "SELECT max(number) FROM blocks;").Scan(&resumeBlock)
    log.Printf("Resuming DB load from block %v", resumeBlock)
    return &dbDataFeed{
      db: ldb,
      startingBlock: uint64(resumeBlock),
      ready: make(chan struct{}),
    }, nil


  } else if strings.HasPrefix(url, "kafka://") {
    return NewKafkaDataFeed(url, db, kafkaRollback, int(finishedLimit))
  } else if strings.HasPrefix(url, "cardinal://") {
		// TODO: Add whitelist support
    return NewCardinalDataFeed(strings.TrimPrefix(url, "cardinal://"), kafkaRollback, finishedLimit, int64(chainid), timestamp, db, nil)
  } else if url == "null://" {
    return &NullDataFeed{}, nil
  }
  return nil, fmt.Errorf("Unknown feed type")
}
