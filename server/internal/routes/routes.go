package routes

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/andrew-aiken/score/internal/handlers"
	"github.com/andrew-aiken/score/internal/middleware"
	"github.com/andrew-aiken/score/internal/static"
)

func StartServer(server *http.Server) error {
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("Failed to start http server", "error", err.Error())
		return fmt.Errorf("start http server: %w", err)
	}

	return nil
}

func SetupRoutes(serverConfig *handlers.Handler, authMiddleware middleware.AuthMiddleware) *http.ServeMux {
	mux := http.NewServeMux()

	staticHandler, err := static.Handler()
	if err != nil {
		slog.Error("Failed to set up static file handler", "error", err.Error())
	} else {
		mux.Handle("/", staticHandler)
	}

	mux.HandleFunc("/auth/verify", handlers.RequireMethod(http.MethodGet, serverConfig.Verify))
	mux.HandleFunc("/auth/login", handlers.RequireMethod(http.MethodPost, serverConfig.Login))

	// Public - returns the NATS websocket address for the frontend
	mux.HandleFunc("/api/config", handlers.RequireMethod(http.MethodGet, serverConfig.Config))

	mux.HandleFunc("/api/checks/mutable-fields", authMiddleware.RequireAuth(handlers.RequireMethod(http.MethodGet, serverConfig.GetMutableFields)))
	mux.HandleFunc("/api/checks", authMiddleware.RequireAuth(handlers.RequireMethod(http.MethodGet, serverConfig.Checks)))
	mux.HandleFunc("/api/settings", authMiddleware.RequireAuth(serverConfig.TeamSettings))

	mux.HandleFunc("/api/admin/settings", authMiddleware.RequireAdminAuth(handlers.RequireMethod(http.MethodGet, serverConfig.ChecksJSON)))
	mux.HandleFunc("/api/admin/cron/start", authMiddleware.RequireAdminAuth(handlers.RequireMethod(http.MethodPut, serverConfig.StartScoringCron)))
	mux.HandleFunc("/api/admin/cron/stop", authMiddleware.RequireAdminAuth(handlers.RequireMethod(http.MethodPut, serverConfig.StopScoringCron)))

	return mux
}
