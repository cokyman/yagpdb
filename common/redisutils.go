package common

import (
	"encoding/json"
	"database/sql"

	"github.com/mediocregopher/radix/v3"
)

// GetRedisJson executes a get redis command and unmarshals the value into out
func GetRedisJson(key string, out interface{}) error {
	var resp []byte
	err := RedisPool.Do(radix.Cmd(&resp, "GET", key))
	if err != nil {
		return err
	}

	if len(resp) == 0 {
		return nil
	}

	err = json.Unmarshal(resp, out)
	return err
}

// SetRedisJson marshals the value and runs a set redis command for key
func SetRedisJson(key string, value interface{}) error {
	serialized, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = RedisPool.Do(radix.Cmd(nil, "SET", key, string(serialized)))
	return err
}

func MultipleCmds(cmds ...radix.CmdAction) error {
	for _, v := range cmds {
		err := RedisPool.Do(v)
		if err != nil {
			return err
		}
	}

	return nil
}

// Utility functions for working with the database connection
func QueryDatabase(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	return db.Query(query, args...)
}

func ExecDatabase(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	return db.Exec(query, args...)
}

// Utility functions for working with the redis connection
func QueryRedis(pool *radix.Pool, cmd string, args ...string) (string, error) {
	var result string
	err := pool.Do(radix.Cmd(&result, cmd, args...))
	return result, err
}

func ExecRedis(pool *radix.Pool, cmd string, args ...string) error {
	return pool.Do(radix.Cmd(nil, cmd, args...))
}
