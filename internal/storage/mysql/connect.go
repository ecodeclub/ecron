package mysql

import (
	_ "github.com/go-sql-driver/mysql"

	"github.com/gotomicro/eorm"
)

var db *eorm.DB

func init() {
	var err error
	db, err = eorm.Open("mysql", "root:@tcp(localhost:3306)/ecron")
	if err != nil {
		panic(err)
	}
	if err = db.Wait(); err != nil {
		panic(err)
	}
}

type Task struct {
	Id              int64 `eorm:"auto_increment,primary_key"`
	Name            string
	SchedulerStatus string
	ExecuteStatus   string
	Cron            string
	Type            string
	Config          string
}
