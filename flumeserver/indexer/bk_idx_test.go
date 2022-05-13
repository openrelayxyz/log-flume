package indexer

import (
	"bytes"
	"testing"
	"fmt"
	"strings"

	"compress/gzip"
	"database/sql"
	"encoding/json"
	"github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	_ "net/http/pprof"

	// log "github.com/inconshreveable/log15"
  "github.com/openrelayxyz/cardinal-streams/delivery"
  "github.com/openrelayxyz/cardinal-streams/transports"
)

func openControlDatabase(connection, file string) (*sql.DB, error) {
	sql.Register(fmt.Sprintf("sqlite3_%v", connection),
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'control';", file), nil)
				return nil
			},
		})
	memDB, err := sql.Open(fmt.Sprintf("sqlite3_%v", connection), ":memory:")
	if err != nil {return nil, err}
	memDB.SetConnMaxLifetime(0)
  memDB.SetMaxIdleConns(32)
	return memDB, nil
}

func pendingBatchDecompress() ([]*delivery.PendingBatch, error) {
  file, _ := ioutil.ReadFile("flume-test-data.json.gz")
  r, err := gzip.NewReader(bytes.NewReader(file))
  if err != nil {
    return nil, err
  }
  raw, _ := ioutil.ReadAll(r)
  if err == io.EOF || err == io.ErrUnexpectedEOF {
    return nil, err
  }
  var transportsObjectSlice []*transports.TransportBatch
  json.Unmarshal(raw, &transportsObjectSlice)
  //use make to set length of secondary list
  pbSlice := []*delivery.PendingBatch{}
  for _, item := range transportsObjectSlice {
    pb := item.ToPendingBatch()
    pbSlice = append(pbSlice, pb)
  }
  return pbSlice, nil
}

func TestBlockIndexer(t *testing.T) {
	controlDB, err := openControlDatabase("bk", "../../blocks.sqlite")
	if err != nil {t.Fatalf(err.Error())}
	_, err = controlDB.Exec(`CREATE TABLE blocks (
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
	if err != nil {t.Fatalf(err.Error())}

	batches, err := pendingBatchDecompress()
	if err != nil {t.Fatalf(err.Error())}

	b := NewBlockIndexer(1)

	statements := make([]string, len(batches))
	for _, pb := range batches {
		group, err := b.Index(pb)
		if err != nil {t.Fatalf(err.Error())}
		statements = append(statements, group...)
	}

	megaStatement := strings.Join(statements, ";")
	_, err = controlDB.Exec(megaStatement)
	if err != nil {t.Fatalf(err.Error())}

	// for i, statement := range statements {
	// 	_, err := controlDB.Exec(statement) //TODO: need to come  back here, make mega string exec mega
	// 	if err != nil {t.Fatalf("error: %v, statement: %v, index: %v",err.Error(), statement, i) }
	// }

	query := "SELECT b.number = blocks.number, b.hash = blocks.hash, b.parentHash = blocks.parentHash, b.uncleHash = blocks.uncleHash, b.coinbase = blocks.coinbase, b.root = blocks.root, b.txRoot = blocks.txRoot, b.receiptRoot = blocks.receiptRoot, b.bloom = blocks.bloom, b.difficulty = blocks.difficulty, b.gasLimit = blocks.gasLimit, b.gasUsed = blocks.gasUsed, b.time = blocks.time, b.extra = blocks.extra, b.mixDigest = blocks.mixDigest, b.nonce = blocks.Nonce, b.uncles = blocks.uncles, b.size =  blocks.size, b.td = blocks.td, b.baseFee = blocks.baseFee FROM blocks INNER JOIN control.blocks as b on blocks.number = b.number"
	rows, err := controlDB.Query(query)
	if err != nil{
		t.Fatalf(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
	var result bool
	err = rows.Scan(&result)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !result {
		t.Fatalf("nope")
	}
}
err = rows.Err()
if err != nil {
	t.Fatalf(err.Error())
}

}
