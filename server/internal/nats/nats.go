package nats

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/andrew-aiken/score/internal/score"
	"github.com/andrew-aiken/score/internal/settings"

	"github.com/nats-io/nats.go"
)

type NatsConnection struct {
	NatsUrl            string
	NatsConnectionName string
	NatsCredsFile      string
	NatsInboxPrefix    string
	NatsConn           *nats.Conn
	NatsKV             nats.KeyValue
	NatsUsersKV        nats.KeyValue
	NatsKVWatchers     []nats.KeyWatcher
	JetStreamConn      nats.JetStreamContext
	Retries            uint8
	natsStreamSub      *nats.Subscription
	scoreEvents        sync.WaitGroup
}

func (n *NatsConnection) SetupConnection() error {
	err := n.natsConnect()
	if err != nil {
		return err
	}

	err = n.jetStreamConnect()
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %v", err)
	}

	if err = n.keyValueConnect(); err != nil {
		return err
	}

	return nil
}

func (n *NatsConnection) natsConnect() error {
	var nc *nats.Conn
	var err error

	var natsRetry = n.Retries
	if n.Retries == 0 {
		natsRetry = 3
	}

	slog.Debug("Connecting to NATS " + n.NatsUrl)

	// Build connection options
	opts := []nats.Option{
		nats.Name(n.NatsConnectionName),
	}
	if n.NatsCredsFile != "" {
		opts = append(opts, nats.UserCredentials(n.NatsCredsFile))
	}

	if n.NatsInboxPrefix != "" {
		opts = append(opts, nats.CustomInboxPrefix(n.NatsInboxPrefix))
	}

	// Connect to NATS with retry
	for i := range natsRetry {
		nc, err = nats.Connect(n.NatsUrl, opts...)
		if err == nil {
			break
		}
		slog.Warn(fmt.Sprintf("Failed to connect to NATS (attempt %d/%d): %v", i+1, natsRetry, err))
		time.Sleep(time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to NATS after %d attempts: %v", natsRetry, err)
	}
	slog.Info("Connected to NATS")

	n.NatsConn = nc

	return nil
}

func (n *NatsConnection) jetStreamConnect() error {
	js, err := n.NatsConn.JetStream()
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %v", err)
	}

	n.JetStreamConn = js
	return nil
}

// keyValueConnect connects to JetStream settings KV and checks if the settings value is present
func (n *NatsConnection) keyValueConnect() error {
	// Get the KV bucket
	kv, err := n.JetStreamConn.KeyValue("settings")
	if err != nil {
		return fmt.Errorf("failed to get KV bucket 'settings': %v", err)
	}

	n.NatsKV = kv

	return nil
}

// SetupUsersKV connects to the "users" KV bucket
func (n *NatsConnection) SetupUsersKV() error {
	kv, err := n.JetStreamConn.KeyValue("users")
	if err != nil {
		return fmt.Errorf("failed to get KV bucket 'users': %v", err)
	}

	n.NatsUsersKV = kv

	return nil
}

// SetupKVWatcher creates one watcher per key
func (n *NatsConnection) SetupKVWatcher(keys []string) error {
	watchers := make([]nats.KeyWatcher, 0, len(keys))

	for _, key := range keys {
		w, err := n.NatsKV.Watch(key)
		if err != nil {
			for _, created := range watchers {
				_ = created.Stop() // best-effort cleanup before returning the original error
			}
			return fmt.Errorf("failed to start KV watcher for key %q: %v", key, err)
		}
		watchers = append(watchers, w)
	}

	n.NatsKVWatchers = watchers

	return nil
}

// SubjectSubscribe subscribes to the ephemeral score trigger stream
func (n *NatsConnection) SubjectSubscribe(ctx context.Context, settings *settings.Settings) error {
	var err error

	var scoreStream = "events.score.>"

	n.natsStreamSub, err = n.NatsConn.Subscribe(scoreStream, score.HandleScoreEvent(ctx, settings, n.JetStreamConn, &n.scoreEvents))
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream %v", err)
	}

	slog.Debug(fmt.Sprintf("Subscribed to events on '%s'", scoreStream))

	return nil
}

// Close closes open nats connections
func (n *NatsConnection) Close() {
	// Close KV watchers
	for _, w := range n.NatsKVWatchers {
		if err := w.Stop(); err != nil {
			slog.Error("Failed to stop key/value watcher", "error", err.Error())
		}
	}
	n.NatsKVWatchers = nil

	// Close stream subscription
	if n.natsStreamSub != nil {
		if err := n.natsStreamSub.Unsubscribe(); err != nil {
			slog.Error("Failed to unsubscribe from nats stream", "error", err.Error())
		}
		n.natsStreamSub = nil
	}

	// Let running checks publish their results before the connection closes
	n.scoreEvents.Wait()

	// Close nats connection
	if n.NatsConn != nil {
		n.NatsConn.Close()
		n.NatsConn = nil
	}
}
