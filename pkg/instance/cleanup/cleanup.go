package instance_cleanup

import (
	"context"
	"fmt"
	"time"

	instance_service "github.com/EvolutionAPI/evolution-go/pkg/instance/service"
	"github.com/gomessguii/logger"
)

const (
	DefaultRetention = 7 * 24 * time.Hour
	DefaultInterval  = time.Hour
)

type CleanupService interface {
	CleanupExpiredDisconnected(now time.Time, retention time.Duration) (instance_service.CleanupResult, error)
}

type Cleaner struct {
	service   CleanupService
	retention time.Duration
	interval  time.Duration
	now       func() time.Time
}

func New(service CleanupService, retention, interval time.Duration) (*Cleaner, error) {
	if service == nil {
		return nil, fmt.Errorf("cleanup service is required")
	}
	if retention <= 0 {
		return nil, fmt.Errorf("retention must be positive")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("cleanup interval must be positive")
	}
	return &Cleaner{
		service:   service,
		retention: retention,
		interval:  interval,
		now:       time.Now,
	}, nil
}

func (c *Cleaner) Sweep() (instance_service.CleanupResult, error) {
	return c.service.CleanupExpiredDisconnected(c.now().UTC(), c.retention)
}

func (c *Cleaner) Run(ctx context.Context) {
	c.sweepAndLog()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.sweepAndLog()
		}
	}
}

func (c *Cleaner) sweepAndLog() {
	result, err := c.Sweep()
	if result.TrackingInitialized > 0 {
		logger.LogInfo(
			"[INSTANCE_CLEANUP] Started the seven-day disconnect clock for %d existing instances",
			result.TrackingInitialized,
		)
	}
	if result.Deleted > 0 {
		logger.LogInfo("[INSTANCE_CLEANUP] Deleted %d expired disconnected instances", result.Deleted)
	}
	if err != nil {
		logger.LogError(
			"[INSTANCE_CLEANUP] Sweep completed with %d failures after %d deletions: %v",
			result.Failed,
			result.Deleted,
			err,
		)
	}
}
