package mqueue

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/botlabs-gg/yagpdb/v2/common"
	"github.com/botlabs-gg/yagpdb/v2/common/backgroundworkers"
	"github.com/mediocregopher/radix/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var _ backgroundworkers.BackgroundWorkerPlugin = (*Plugin)(nil)

// RunBackgroundWorker implements backgroundworkers.BackgroundWorkerPlugin
func (p *Plugin) RunBackgroundWorker() {
	t := time.NewTicker(time.Second * 5)
	for range t.C {
		p.updateMetrcis()
	}
}

// StopBackgroundWorker implements backgroundworkers.BackgroundWorkerPlugin
func (p *Plugin) StopBackgroundWorker(wg *sync.WaitGroup) {
	wg.Done()
}

func (p *Plugin) updateMetrcis() {
	var n int64
	err := common.RedisPool.Do(radix.Cmd(&n, "ZCARD", "mqueue"))
	if err != nil {
		logger.WithError(err).Error("failed updating mqueue metrics")
	}

	metricsQueueSize.Set(float64(n))
}

var metricsQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "yagpdb_mqueue_size_total",
	Help: "The size of the send message queue",
})

// Background worker to process database queries
func (p *Plugin) processDatabaseQueries(ctx context.Context, db *sql.DB) {
	t := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-t.C:
			// Process database queries here
		case <-ctx.Done():
			return
		}
	}
}

// Background worker to process redis commands
func (p *Plugin) processRedisCommands(ctx context.Context, redis *radix.Pool) {
	t := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-t.C:
			// Process redis commands here
		case <-ctx.Done():
			return
		}
	}
}
