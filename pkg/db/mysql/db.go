package mysql

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLOptions struct {
	Host                  string
	Username              string
	Password              string
	Database              string
	MaxIdleConnections    int
	MaxOpenConnections    int
	MaxConnectionLifeTime time.Duration
	LogLevel              int
}

func (o *MySQLOptions) DSN() string {
	return fmt.Sprintf(`%s:%s@tcp(%s)/%s?charset=utf8&parseTime=%t&loc=%s`,
		o.Username,
		o.Password,
		o.Host,
		o.Database,
		true,
		"Local")
}

func NewMySQL(opts *MySQLOptions) (*sql.DB, error) {
	db, err := sql.Open("mysql", opts.DSN())

	if err != nil {
		return nil, err
	}

	// SetMaxOpenConns 设置到数据库的最大打开连接数
	db.SetMaxOpenConns(opts.MaxOpenConnections)

	// SetConnMaxLifetime 设置连接可重用的最长时间
	db.SetConnMaxLifetime(opts.MaxConnectionLifeTime)

	// SetMaxIdleConns 设置空闲连接池的最大连接数
	db.SetMaxIdleConns(opts.MaxIdleConnections)

	return db, nil
}
