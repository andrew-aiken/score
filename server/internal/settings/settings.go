package settings

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/andrew-aiken/checks"

	"github.com/nats-io/nats.go"
)

// maxEntrySize bounds how large a single KV entry's value may be before it is unmarshaled.
const maxEntrySize = 1 << 20 // 1MB

// TeamState contains information about an individuals team config
type TeamState struct {
	mu         sync.RWMutex
	Attributes map[string]map[string]string
	StaticConf checks.StaticConf
}

// GetAttributes returns the override attributes for a single check, safe for concurrent use.
func (ts *TeamState) GetAttributes(checkName string) map[string]string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.Attributes[checkName]
}

type Settings struct {
	mu     sync.RWMutex
	Checks map[string]Check
	Teams  map[uint16]*TeamState
}

// GetCheck returns the named check, safe for concurrent use.
func (s *Settings) GetCheck(name string) (Check, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.Checks[name]
	return c, ok
}

// watcherUpdate pairs one watcher's Updates() value with whether its channel is still open, mirroring the two-value form of a channel receive.
type watcherUpdate struct {
	entry nats.KeyValueEntry
	ok    bool
}

// mergeWatchers fans multiple watchers' Updates() channels into one
func mergeWatchers(watchers []nats.KeyWatcher, done <-chan struct{}) (outChan <-chan watcherUpdate) {
	merged := make(chan watcherUpdate)

	var wg sync.WaitGroup
	for _, w := range watchers {
		wg.Add(1)
		go func(w nats.KeyWatcher) {
			defer wg.Done()
			updates := w.Updates()
			for {
				select {
				case entry, ok := <-updates:
					if !ok {
						select {
						case merged <- watcherUpdate{ok: false}:
						case <-done:
						}
						return
					}
					select {
					case merged <- watcherUpdate{entry: entry, ok: true}:
					case <-done:
						return
					}
				case <-done:
					return
				}
			}
		}(w)
	}

	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}

// MonitorSettings loops monitoring the nats KV for settings updates across one or more watchers
// Returns nil when ctx is cancelled and an error if the watchers stop while still needed
func (s *Settings) MonitorSettings(ctx context.Context, watchers []nats.KeyWatcher) error {
	s.mu.Lock()
	if s.Checks == nil {
		s.Checks = make(map[string]Check)
	}
	s.mu.Unlock()

	done := make(chan struct{})
	defer close(done)

	merged := mergeWatchers(watchers, done)

	// Each watcher reports its own "initial sync done" marker
	// Flips startUpCompleted once every watcher has reported in.
	pendingInit := len(watchers)
	startUpCompleted := pendingInit == 0

	for {
		select {
		case <-ctx.Done():
			slog.Warn("Context cancelled, stopping watcher")
			return nil
		case update, chanOpen := <-merged:
			if !chanOpen {
				if len(watchers) == 0 {
					return nil
				}
				return fmt.Errorf("NATS KV watchers stopped")
			}
			if !update.ok {
				slog.Error("NATS KV watcher failed")
				return fmt.Errorf("NATS KV watcher failed")
			}
			entry := update.entry

			if entry == nil {
				pendingInit--
				if pendingInit == 0 {
					slog.Info("Initial KV sync complete, switching to watch for updates")
					startUpCompleted = true
				}
				continue
			}

			key := entry.Key()
			value := entry.Value()

			if len(value) > maxEntrySize {
				slog.Warn("Skipping oversized KV entry", "key", key, "size", len(value))
				continue
			}

			checkName, isCheck := strings.CutPrefix(key, "check.")

			switch {
			case isCheck:
				// If the check is removed drop from the list of loaded checks and continue
				if entry.Operation() != nats.KeyValuePut {
					// If initial startup has not completed skip removing checks
					if !startUpCompleted {
						continue
					}

					s.mu.Lock()
					delete(s.Checks, checkName)
					s.mu.Unlock()
					slog.Info("Removed check", "check", checkName)
					continue
				}

				slog.Info("Loading check", "check", checkName)

				check := Check{}
				if err := json.Unmarshal(value, &check); err != nil {
					slog.Warn("Failed to unmarshal settings for check", "check", checkName, "error", err)
					continue
				}

				s.mu.Lock()
				s.Checks[checkName] = check
				s.mu.Unlock()
			default:
				// Check if the key if formatted as X.settings
				// At this time only checks.X and X.settings are valid in the KV
				settingsTeamID, found := strings.CutSuffix(key, ".settings")
				if !found {
					slog.Warn("Unknown key", "key", key)
					continue
				}

				teamID, err := strconv.ParseUint(settingsTeamID, 10, 16)
				if err != nil {
					slog.Warn("Invalid team ID", "teamID", settingsTeamID, "error", err)
					continue
				}

				s.mu.RLock()
				teamState, exists := s.Teams[uint16(teamID)]
				s.mu.RUnlock()

				if exists {
					// If the nats change updates the setting update the stored value
					if entry.Operation() == nats.KeyValuePut {
						var teamSettings map[string]map[string]string
						if err := json.Unmarshal(value, &teamSettings); err != nil {
							slog.Warn("Failed to unmarshal team settings", "key", key, "error", err)
							continue
						}

						teamState.mu.Lock()
						teamState.Attributes = teamSettings
						teamState.mu.Unlock()
						slog.Info("Team settings update", "team", teamID)
					} else { // If not updating its removing: drop the attributes key
						// If initial startup has not completed skip removing checks
						if !startUpCompleted {
							continue
						}

						teamState.mu.Lock()
						teamState.Attributes = nil
						teamState.mu.Unlock()
						slog.Info("Removed team settings", "team", teamID)
					}
				} else {
					slog.Warn("Unknown team key", "key", key)
				}
			}
		}
	}
}
