package lib

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type DatabaseConfig struct {
	Host                  string `json:"host"`
	Port                  string `json:"port"`
	User                  string `json:"user"`
	Password              string `json:"password"`
	Database              string `json:"database"`
	MaxOpenConnections    *int   `json:"max_open_connections"`
	MaxIdleConnections    *int   `json:"max_idle_connections"`
	ConnectionMaxLifetime *int   `json:"connection_max_lifetime"`
	ConnectionMaxIdleTime *int   `json:"connection_max_idle_time"`
}

type DatabaseClient struct {
	Config     DatabaseConfig
	Connection *sql.DB
}

const (
	DefaultMaxOpenConnections    = 25
	DefaultMaxIdleConnections    = 5
	DefaultConnectionMaxLifetime = 300
	DefaultConnectionMaxIdleTime = 60
)

func NewDatabaseClient(config DatabaseConfig) *DatabaseClient {
	return &DatabaseClient{
		Config: config,
	}
}

func (c *DatabaseClient) Connect() error {
	if c.Connection != nil {
		return nil
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		c.Config.User,
		c.Config.Password,
		c.Config.Host,
		c.Config.Port,
		c.Config.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	maxOpenConnections := DefaultMaxOpenConnections
	if c.Config.MaxOpenConnections != nil {
		maxOpenConnections = *c.Config.MaxOpenConnections
	}
	db.SetMaxOpenConns(maxOpenConnections)

	maxIdleConnections := DefaultMaxIdleConnections
	if c.Config.MaxIdleConnections != nil {
		maxIdleConnections = *c.Config.MaxIdleConnections
	}
	db.SetMaxIdleConns(maxIdleConnections)

	connectionMaxLifetime := DefaultConnectionMaxLifetime
	if c.Config.ConnectionMaxLifetime != nil {
		connectionMaxLifetime = *c.Config.ConnectionMaxLifetime
	}
	db.SetConnMaxLifetime(time.Duration(connectionMaxLifetime) * time.Second)

	connectionMaxIdleTime := DefaultConnectionMaxIdleTime
	if c.Config.ConnectionMaxIdleTime != nil {
		connectionMaxIdleTime = *c.Config.ConnectionMaxIdleTime
	}
	db.SetConnMaxIdleTime(time.Duration(connectionMaxIdleTime) * time.Second)

	if err = db.Ping(); err != nil {
		if err = db.Close(); err != nil {
			return err
		}
		return err
	}

	c.Connection = db

	return nil
}

func (c *DatabaseClient) Disconnect() error {
	if c.Connection != nil {
		return c.Connection.Close()
	}

	return nil
}
