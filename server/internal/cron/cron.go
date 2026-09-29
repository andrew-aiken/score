package cron

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/nats-io/nats.go"
)

func AddCheckCron(cron gocron.Scheduler, natsConnection *nats.Conn, checkName string, checkFrequency uint16) (job gocron.Job, err error) {
	if checkFrequency == 0 {
		err = fmt.Errorf("cron frequency cannot be zero")
		return
	}

	// Remove any existing job for with this check name so updates replace it instead of duplicating it.
	cron.RemoveByTags(checkName)

	return cron.NewJob(
		gocron.DurationJob(time.Duration(checkFrequency)*time.Second),
		gocron.NewTask(
			func(checkName string) {
				slog.Debug("Published check trigger", "check", checkName)
				err := natsConnection.Publish("events.score."+checkName, []byte{})
				if err != nil {
					slog.Error("Failed to publish check trigger", "error", err.Error())
				}
			},
			checkName,
		),
		gocron.WithTags(checkName),
	)
}

func RemoveCheckCron(cron gocron.Scheduler, checkName string) {
	cron.RemoveByTags(checkName)
}
