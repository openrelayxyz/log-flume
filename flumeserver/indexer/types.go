package indexer

import (
	"github.com/openrelayxyz/cardinal-streams/delivery"
)

type Indexer interface {
	Index(*delivery.PendingBatch) ([]string, error)
}
