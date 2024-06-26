package storage

import "github.com/ecodeclub/ecron/internal/storage/mysql"

type MysqlStorage struct {
	mysql.GormTaskDAO
}

type MysqlHistoryStorage struct {
	mysql.GormHistoryDAO
}
