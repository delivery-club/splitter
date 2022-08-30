package splitter

import (
    "errors"
    "time"
)

const handlerName = "Splitter.Sync"

const (
    storageError      = "storage_error"
    actualizeConflict = "actualize_conflict"
)

var ErrNoEntries = errors.New("no entries")

type PodDTO struct {
    ID        int64
    Num       int64
    Group     string
    CreatedAt time.Time
    UpdatedAt time.Time
}
