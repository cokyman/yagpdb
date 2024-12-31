package common

import (
	"database/sql"
	"strconv"
	"time"

	"emperror.dev/errors"
	"github.com/mediocregopher/radix/v3"
)

// Locks the lock and if succeded sets it to expire after maxdur
// So that if someting went wrong its not locked forever
func TryLockRedisKey(key string, maxDur int) (bool, error) {
	resp := ""
	err := RedisPool.Do(radix.Cmd(&resp, "SET", key, "1", "NX", "EX", strconv.Itoa(maxDur)))
	if err != nil {
		return false, err
	}

	if resp == "OK" {
		return true, nil
	}

	return false, nil
}

var (
	ErrMaxLockAttemptsExceeded = errors.New("Max lock attempts exceeded")
)

// BlockingLockRedisKey blocks until it suceeded to lock the key
func BlockingLockRedisKey(key string, maxTryDuration time.Duration, maxLockDur int) error {
	started := time.Now()
	sleepDur := time.Millisecond * 100
	maxSleep := time.Second
	for {
		if maxTryDuration != 0 && time.Since(started) > maxTryDuration {
			return ErrMaxLockAttemptsExceeded
		}

		locked, err := TryLockRedisKey(key, maxLockDur)
		if err != nil {
			return ErrWithCaller(err)
		}

		if locked {
			return nil
		}

		time.Sleep(sleepDur)
		sleepDur *= 2
		if sleepDur > maxSleep {
			sleepDur = maxSleep
		}
	}
}

func UnlockRedisKey(key string) {
	for {
		err := RedisPool.Do(radix.Cmd(nil, "DEL", key))
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		break
	}
}

// Locks the lock in the database and if succeeded sets it to expire after maxDur
// So that if something went wrong it's not locked forever
func TryLockDatabaseKey(db *sql.DB, key string, maxDur int) (bool, error) {
	query := "INSERT INTO locks (key, expires_at) VALUES ($1, NOW() + INTERVAL '1 second' * $2) ON CONFLICT (key) DO UPDATE SET expires_at = EXCLUDED.expires_at WHERE locks.expires_at < NOW()"
	result, err := db.Exec(query, key, maxDur)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected > 0, nil
}

// BlockingLockDatabaseKey blocks until it succeeded to lock the key in the database
func BlockingLockDatabaseKey(db *sql.DB, key string, maxTryDuration time.Duration, maxLockDur int) error {
	started := time.Now()
	sleepDur := time.Millisecond * 100
	maxSleep := time.Second
	for {
		if maxTryDuration != 0 && time.Since(started) > maxTryDuration {
			return ErrMaxLockAttemptsExceeded
		}

		locked, err := TryLockDatabaseKey(db, key, maxLockDur)
		if err != nil {
			return ErrWithCaller(err)
		}

		if locked {
			return nil
		}

		time.Sleep(sleepDur)
		sleepDur *= 2
		if sleepDur > maxSleep {
			sleepDur = maxSleep
		}
	}
}

func UnlockDatabaseKey(db *sql.DB, key string) {
	for {
		_, err := db.Exec("DELETE FROM locks WHERE key = $1", key)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		break
	}
}
