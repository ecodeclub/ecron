package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocker_Acquire(t *testing.T) {
	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/ecron")
	assert.Nil(t, err)

	lock := newLocker("lookup")

	err = lock.Acquire(context.TODO())
	assert.Nil(t, err)

	go selectOther(db)

	fmt.Println("release")
	err = lock.Release()
	assert.Nil(t, err)

	time.Sleep(time.Second)
}

func selectOther(db *sql.DB) {
	rows, err := db.Query("SELECT * FROM `ecron_lock` WHERE lock_name = ? FOR UPDATE", "lookup")
	if err != nil {
		panic(err)
	}
	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		if err != nil {
			panic(err)
		}
		fmt.Println(name)
	}
}
