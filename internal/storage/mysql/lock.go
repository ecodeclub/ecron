package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Locker struct {
	name string
	tx   *sql.Tx
	db   *sql.DB
}

func newLocker(name string) *Locker {
	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/ecron")
	if err != nil {
		panic(err)
	}
	return &Locker{name: name, db: db}
}

func (l *Locker) Acquire(ctx context.Context) error {
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	l.tx = tx
	rows, err := tx.QueryContext(ctx, "SELECT * FROM `ecron_lock` where lock_name = ? FOR UPDATE", l.name)
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		count += 1
	}
	if count != 1 {
		return errors.New(fmt.Sprintf("lock: 锁数量异常: %d", count))
	}
	return nil
}

func (l *Locker) Release() error {
	return l.tx.Commit()
}
