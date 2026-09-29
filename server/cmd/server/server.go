package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/andrew-aiken/score/internal/auth"
	"github.com/andrew-aiken/score/internal/config"
	"github.com/andrew-aiken/score/internal/cron"
	"github.com/andrew-aiken/score/internal/handlers"
	"github.com/andrew-aiken/score/internal/logging"
	"github.com/andrew-aiken/score/internal/middleware"
	"github.com/andrew-aiken/score/internal/nats"
	"github.com/andrew-aiken/score/internal/routes"
	"github.com/andrew-aiken/score/internal/settings"
	"github.com/andrew-aiken/score/internal/sink"

	"github.com/go-co-op/gocron/v2"
	natsnats "github.com/nats-io/nats.go"
)

type ServerArgs struct {
	Context        context.Context
	LogLevel       string
	ConfigFilePath string
	NatsAddress    string
	NatsCreds      string
	NatsPublicURL  string
	DB             bool
	DBPath         string
}

// Server setups the initial connection to NATS, watches checks that get loaded into cron, and runs the webserver
func Server(args ServerArgs) (err error) {
	logging.SetupLogging(args.LogLevel)

	parent := args.Context
	if parent == nil {
		parent = context.Background()
	}

	// Load configuration
	cfg, err := config.Load(args.ConfigFilePath)
	if err != nil {
		slog.Error("Failed to load configuration", "error", err.Error())
		return fmt.Errorf("load config: %w", err)
	}

	cronScheduler, err := gocron.NewScheduler()
	if err != nil {
		slog.Error("Failed to create cron scheduler", "error", err.Error())
		return fmt.Errorf("create cron scheduler: %w", err)
	}
	defer func() {
		err := cronScheduler.Shutdown()
		if err != nil {
			slog.Error("Error shutting down the cron scheduler", "error", err.Error())
		}
	}()

	// Initialize NATS auth service
	natsAuthService, err := auth.NewNATSAuthService(cfg.AccountSigningSeed, cfg.AccountPublicKey)
	if err != nil {
		slog.Error("Failed to initialize NATS auth service", "error", err.Error())
		return fmt.Errorf("initialize NATS auth service: %w", err)
	}

	// Initialize NATS KV client
	natsClient := nats.NatsConnection{
		NatsUrl:       args.NatsAddress,
		NatsCredsFile: args.NatsCreds,
		Retries:       30,
	}
	err = natsClient.SetupConnection()
	if err != nil {
		slog.Error("Failed to initialize NATS KV client")
		return err
	} else {
		slog.Debug("Connected to NATS KV bucket")
		defer natsClient.Close()
	}

	if err := natsClient.SetupUsersKV(); err != nil {
		slog.Error("Failed to initialize NATS users KV client")
		return err
	}

	consumerErrCh := make(chan error, 1)
	var resultsDB *sink.DB
	consumerCtx, cancelConsumer := context.WithCancel(parent)
	defer cancelConsumer()

	if args.DB {
		resultsDB, err = sink.Open(args.DBPath)
		if err != nil {
			slog.Error("Failed to open results database", "error", err.Error())
			return fmt.Errorf("open results database: %w", err)
		}
		defer func() {
			dbCloseErr := resultsDB.Close()
			if err == nil && dbCloseErr != nil {
				err = dbCloseErr
			}
		}()

		go func() {
			consumerErrCh <- sink.Consume(consumerCtx, natsClient.JetStreamConn, resultsDB)
		}()
		slog.Info("Consuming results stream into SQLite", "path", args.DBPath)
	}

	kv := natsClient.NatsKV
	kvWatcher, err := kv.Watch("check.*")
	if err != nil {
		slog.Error("Failed to start KV watcher")
		return err
	}
	defer func() {
		err := kvWatcher.Stop()
		if err != nil {
			slog.Error("Error stopping NATS key watcher", "error", err.Error())
		}
	}()

	// Watch for check updates in background
	go monitorChecks(kvWatcher, cronScheduler, natsClient.NatsConn)
	slog.Info("Watching KV for check changes")

	// Create handler
	h := &handlers.Handler{
		NatsAuthService:   natsAuthService,
		NatsKVClient:      natsClient.NatsKV,
		NatsUsersKVClient: natsClient.NatsUsersKV,
		CronScheduler:     cronScheduler,
		NatsPublicURL:     args.NatsPublicURL,
	}

	authMiddleware := middleware.NewAuthMiddleware(natsAuthService)

	mux := routes.SetupRoutes(h, *authMiddleware)

	server := &http.Server{
		Addr:         "0.0.0.0:" + strconv.Itoa(cfg.HttpPort),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	serverErrCh := make(chan error, 1)

	// Start server in the background
	go func() {
		serverErrCh <- routes.StartServer(server)
	}()
	slog.Info(fmt.Sprintf("Started webserver, listening on port %d", cfg.HttpPort))

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	select {
	case err := <-serverErrCh:
		if err != nil {
			return err
		}
		return nil
	case err := <-consumerErrCh:
		if err != nil {
			return err
		}
		return nil
	case sig := <-sigChan:
		slog.Info("Received shutting down trigger...", "signal", sig)
	case <-parent.Done():
		slog.Info("Received shutdown trigger from context")
	}

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop KV watcher if running
	if kvWatcher != nil {
		slog.Debug("Stopping KV watcher")
		err = kvWatcher.Stop()
		if err != nil {
			slog.Error("Error stopping NATS KV watcher")
			return err
		}
	}

	// Shutdown HTTP server
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("Shutting down http server")
		return err
	}

	// Stop the results consumer, if running, and wait for it to drain
	if args.DB {
		cancelConsumer()
		if err := <-consumerErrCh; err != nil {
			slog.Error("Error stopping results consumer", "error", err.Error())
		}
		if err := resultsDB.Close(); err != nil {
			slog.Error("Error closing results database", "error", err.Error())
		}
	}

	slog.Info("Server stopped")

	return nil
}

// monitorChecks monitors nats kv changes and schedules cronjobs on change
func monitorChecks(kvWatcher natsnats.KeyWatcher, cronScheduler gocron.Scheduler, natsConnection *natsnats.Conn) {
	initialized := false
	for entry := range kvWatcher.Updates() {
		if entry == nil {
			initialized = true
			continue
		}

		// Remove the prefix from the nats KV
		checkName := strings.TrimPrefix(entry.Key(), "check.")

		// Filter based on event type
		switch entry.Operation().String() {
		case "KeyValuePutOp":
			// var check check
			var check settings.Check

			if err := json.Unmarshal(entry.Value(), &check); err != nil {
				slog.Warn("Failed to unmarshal settings", entry.Key(), err)
				continue
			}

			_, err := cron.AddCheckCron(cronScheduler, natsConnection, checkName, check.Frequency)
			if err != nil {
				slog.Warn("Failed to add check to cron", "error", err)
				continue
			}
			slog.Info("Added check to cron", "name", checkName, "total", strconv.Itoa(len(cronScheduler.Jobs())))

		case "KeyValuePurgeOp":
		case "KeyValueDeleteOp":
			if !initialized {
				continue
			}
			cron.RemoveCheckCron(cronScheduler, checkName)
			slog.Info("Removed check from cron", "name", checkName, "total", strconv.Itoa(len(cronScheduler.Jobs())))
		default:
			slog.Warn("Ignoring unknown KV operation", "operation", entry.Operation().String(), "key", entry.Key())
			continue
		}
	}
}
