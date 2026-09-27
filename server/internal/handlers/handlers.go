package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"

	"github.com/andrew-aiken/score/internal/auth"
	"github.com/andrew-aiken/score/internal/middleware"
	"github.com/andrew-aiken/score/internal/nats"

	"github.com/go-co-op/gocron/v2"
	natsnats "github.com/nats-io/nats.go"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	NatsAuthService   *auth.NATSAuthService
	NatsKVClient      natsnats.KeyValue
	NatsUsersKVClient natsnats.KeyValue
	CronScheduler     gocron.Scheduler
	NatsPublicURL     string
}

// Verify validates a NATS JWT token from the Authorization header
func (h *Handler) Verify(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		w.WriteHeader(http.StatusUnauthorized)
		encodeJson(w, map[string]bool{"valid": false})
		return
	}

	tokenString, err := auth.ParseBearerToken(authHeader)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		encodeJson(w, map[string]bool{"valid": false})
		return
	}

	_, err = h.NatsAuthService.VerifyJWT(tokenString)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		encodeJson(w, map[string]bool{"valid": false})
		return
	}

	encodeJson(w, map[string]bool{"valid": true})
}

// Config returns public, unauthenticated runtime configuration for the frontend
func (h *Handler) Config(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encodeJson(w, map[string]string{"natsUrl": h.NatsPublicURL})
}

// GetMutableFields returns a map of check names to their mutable fields from the NATS KV settings bucket
func (h *Handler) GetMutableFields(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	mutableFields, err := nats.GetMutableFields(h.NatsKVClient)
	if err != nil {
		slog.Warn("Failed to get mutable fields", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		encodeJson(w, map[string]string{"error": "Failed to retrieve mutable fields"})
		return
	}

	encodeJson(w, mutableFields)
}

// TeamSettings handles GET and PUT for team-specific settings in NATS KV
// The team number is extracted from the user's JWT roles
func (h *Handler) TeamSettings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get claims from context (set by auth middleware)
	claims := middleware.GetClaimsFromContext(r.Context())
	if claims == nil {
		w.WriteHeader(http.StatusUnauthorized)
		encodeJson(w, map[string]string{"error": "Unauthorized"})
		return
	}

	teamNumber := claims.TeamID

	switch r.Method {
	case http.MethodGet:
		// Fetch team settings
		settings, err := nats.GetTeamSettings(h.NatsKVClient, teamNumber)
		if err != nil {
			slog.Warn("Failed to get team settings", "team", teamNumber, "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			encodeJson(w, map[string]string{"error": "Failed to retrieve settings"})
			return
		}
		encodeJson(w, settings)

	case http.MethodPut:
		// Parse request body
		var settings map[string]map[string]string
		if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			encodeJson(w, map[string]string{"error": "Invalid request body"})
			return
		}

		// Write to NATS KV
		if err := nats.PutTeamSettings(h.NatsKVClient, teamNumber, settings); err != nil {
			slog.Warn("Failed to update team settings", "team", teamNumber, "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			encodeJson(w, map[string]string{"error": "Failed to save settings"})
			return
		}

		slog.Info("Team settings updated", "team", teamNumber, "user", claims.UserID)
		encodeJson(w, map[string]any{
			"success": true,
			"team":    teamNumber,
		})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		encodeJson(w, map[string]string{"error": "Method not allowed"})
	}
}

// Checks returns a sorted list of all check names from the NATS KV settings bucket
func (h *Handler) Checks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	checks, err := nats.GetChecks(h.NatsKVClient)
	if err != nil {
		slog.Warn("Failed to get checks", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		encodeJson(w, map[string]string{"error": "Failed to retrieve checks"})
		return
	}

	names := make([]string, 0, len(checks))
	for name := range checks {
		names = append(names, name)
	}
	sort.Strings(names)

	encodeJson(w, names)
}

// RequireMethod rejects requests whose method does not match method with a 405
func RequireMethod(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.WriteHeader(http.StatusMethodNotAllowed)
			encodeJson(w, map[string]string{"error": "Method not allowed"})
			return
		}
		next(w, r)
	}
}

func encodeJson(writer io.Writer, a any) {
	if err := json.NewEncoder(writer).Encode(a); err != nil {
		slog.Error("Failed to encode handler headers", "error", err.Error())
	}
}
