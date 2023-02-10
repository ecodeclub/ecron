package mysql

import (
	"database/sql"
	"sync"
)

var (
	once sync.Once
	S    *datastore
)

type datastore struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *datastore {
	once.Do(func() {
		S = &datastore{
			db: db,
		}
	})

	return S
}

type IStore interface {
	DB() *sql.DB
}

var _ IStore = (*datastore)(nil)

func (d *datastore) DB() *sql.DB {
	return d.db
}
