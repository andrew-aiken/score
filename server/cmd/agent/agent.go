package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/andrew-aiken/checks"

	"github.com/andrew-aiken/score/internal/logging"
	"github.com/andrew-aiken/score/internal/nats"
	"github.com/andrew-aiken/score/internal/settings"
)

type RunArgs struct {
	LogLevel      string
	NatsUrl       string
	NatsCredsFile string
	TeamNumbers   []uint16
	Context       context.Context
}

func Run(args RunArgs) error {
	logging.SetupLogging(args.LogLevel)

	parent := args.Context
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var connName string
	if len(args.TeamNumbers) == 1 {
		connName = fmt.Sprintf("%d-agent", args.TeamNumbers[0])
	} else {
		connName = "multi-agent"
	}

	var natsCon = nats.NatsConnection{
		NatsUrl:            args.NatsUrl,
		NatsConnectionName: connName,
		NatsCredsFile:      args.NatsCredsFile,
		NatsInboxPrefix:    inboxPrefix(args.TeamNumbers),
		Retries:            30,
	}

	err := natsCon.SetupConnection()
	if err != nil {
		slog.Error("Failed to connect to NATS")
		return err
	}
	defer natsCon.Close()

	watchList := []string{"check.*"}
	for _, n := range args.TeamNumbers {
		watchList = append(watchList, fmt.Sprintf("%d.settings", n))
		slog.Debug("Team Loaded", "team", n)
	}

	if err = natsCon.SetupKVWatcher(watchList); err != nil {
		slog.Error("Failed to start NATS KV Watcher")
		return err
	}

	var agentSettings settings.Settings
	agentSettings.Checks = make(map[string]settings.Check)
	agentSettings.Teams = make(map[uint16]*settings.TeamState, len(args.TeamNumbers))
	for _, n := range args.TeamNumbers {
		agentSettings.Teams[n] = &settings.TeamState{
			Attributes: make(map[string]map[string]string),
			StaticConf: checks.StaticConf{
				TeamNumber:    n,
				TeamNumberHex: fmt.Sprintf("%x", n),
			},
		}
	}

	if err = natsCon.SubjectSubscribe(ctx, &agentSettings); err != nil {
		return err
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	go func() {
		select {
		case <-sigChan:
			slog.Info("Shutting down...")
			cancel()
		case <-ctx.Done():
		}
	}()

	// Process KV settings updates
	agentSettings.MonitorSettings(ctx, natsCon.NatsKVWatchers)

	return nil
}

// inboxPrefix generates inbox prefix to connect with
// Agents use their own "_AGENT" namespace prefix
func inboxPrefix(teamNumbers []uint16) string {
	if len(teamNumbers) == 1 {
		return fmt.Sprintf("_AGENT.%d", teamNumbers[0])
	}
	return "_AGENT.0"
}
