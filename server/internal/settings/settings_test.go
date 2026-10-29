package settings

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// setupSettingsKV spins up a real embedded NATS server with JetStream, creates the "settings" KV bucket, and returns a handle to it.
func setupSettingsKV(t *testing.T) nats.KeyValue {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	opts.MaxPayload = 4 << 20 // large enough for tests that exercise settings.maxEntrySize (1MB)

	server := natsserver.RunServer(&opts)
	t.Cleanup(server.Shutdown)

	nc, err := nats.Connect(server.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Failed to connect to JetStream: %v", err)
	}

	bucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       "settings",
		Description:  "Check & configuration storage",
		History:      5,
		MaxValueSize: -1,
		MaxBytes:     -1,
	})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	kv, err := js.KeyValue(bucket.Bucket())
	if err != nil {
		t.Fatalf("Failed to connect to key value: %v", err)
	}

	return kv
}

// watch creates a real KeyWatcher on pattern, stopped via t.Cleanup.
func watch(t *testing.T, kv nats.KeyValue, pattern string) nats.KeyWatcher {
	t.Helper()

	w, err := kv.Watch(pattern)
	if err != nil {
		t.Fatalf("Failed to watch %q: %v", pattern, err)
	}
	t.Cleanup(func() { _ = w.Stop() })

	return w
}

// watchAll creates a real KeyWatcher across every key in the bucket, stopped via t.Cleanup.
func watchAll(t *testing.T, kv nats.KeyValue) nats.KeyWatcher {
	t.Helper()

	w, err := kv.WatchAll()
	if err != nil {
		t.Fatalf("Failed to watch all keys: %v", err)
	}
	t.Cleanup(func() { _ = w.Stop() })

	return w
}

// waitFor polls cond until it returns true, failing the test if it never does.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// runMonitor starts MonitorSettings in the background and returns a stop func that cancels it and waits for it to return.
func runMonitor(t *testing.T, s *Settings, watchers []nats.KeyWatcher) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.MonitorSettings(ctx, watchers)
	}()

	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("MonitorSettings returned an error after cancellation: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("MonitorSettings did not stop after context cancellation")
		}
	}
}

func noopCheckJSON(name string) []byte {
	data, _ := json.Marshal(map[string]any{
		"name":        name,
		"type":        "noop",
		"scoreWeight": 1,
		"definition":  map[string]any{"pass": true},
	})
	return data
}

func TestGetCheck(t *testing.T) {
	s := &Settings{
		Checks: map[string]Check{
			"noop": {
				Name: "noop",
			},
		},
	}

	if c, ok := s.GetCheck("noop"); !ok || c.Name != "noop" {
		t.Errorf("GetCheck(noop) = %+v, %v; want present with Name noop", c, ok)
	}

	if _, ok := s.GetCheck("missing"); ok {
		t.Error("GetCheck(missing) returned ok=true, want false")
	}
}

func TestTeamStateGetAttributes(t *testing.T) {
	ts := &TeamState{Attributes: map[string]map[string]string{"noop": {"key": "value"}}}

	if got := ts.GetAttributes("noop"); got["key"] != "value" {
		t.Errorf("GetAttributes(noop) = %v, want key=value", got)
	}

	if got := ts.GetAttributes("missing"); got != nil {
		t.Errorf("GetAttributes(missing) = %v, want nil", got)
	}
}

// TestTeamStateGetAttributesConcurrent exercises GetAttributes against concurrent writers; run with -race to catch any unsynchronized access.
func TestTeamStateGetAttributesConcurrent(t *testing.T) {
	ts := &TeamState{Attributes: map[string]map[string]string{"noop": {"key": "value"}}}

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			ts.GetAttributes("noop")
		}()
		go func() {
			defer wg.Done()
			ts.mu.Lock()
			ts.Attributes = map[string]map[string]string{"noop": {"key": "value2"}}
			ts.mu.Unlock()
		}()
	}
	wg.Wait()
}

func TestMergeWatchers(t *testing.T) {
	kv := setupSettingsKV(t)

	t.Run("fans through a watcher's initial sync marker and live updates", func(t *testing.T) {
		w := watch(t, kv, "check.*")
		done := make(chan struct{})
		defer close(done)

		merged := mergeWatchers([]nats.KeyWatcher{w}, done)

		u, ok := <-merged
		if !ok || !u.ok || u.entry != nil {
			t.Fatalf("got %+v, ok=%v; want the nil initial-sync marker", u, ok)
		}

		if _, err := kv.Put("check.a", noopCheckJSON("a")); err != nil {
			t.Fatalf("Failed to put check: %v", err)
		}

		u, ok = <-merged
		if !ok || !u.ok || u.entry == nil || u.entry.Key() != "check.a" {
			t.Fatalf("got %+v, ok=%v; want a put for check.a", u, ok)
		}
	})

	t.Run("closes merged once every watcher's channel closes", func(t *testing.T) {
		w := watch(t, kv, "0.settings")
		done := make(chan struct{})
		defer close(done)

		merged := mergeWatchers([]nats.KeyWatcher{w}, done)

		<-merged // drain the initial sync marker

		if err := w.Stop(); err != nil {
			t.Fatalf("Failed to stop watcher: %v", err)
		}

		u, ok := <-merged
		if !ok {
			t.Fatal("expected a watcherUpdate signaling the closed watcher before merged closes")
		}
		if u.ok {
			t.Error("expected ok=false for a closed watcher channel")
		}

		if _, ok := <-merged; ok {
			t.Error("expected merged to be closed after the only watcher finished")
		}
	})

	t.Run("no watchers closes merged immediately", func(t *testing.T) {
		done := make(chan struct{})
		defer close(done)

		merged := mergeWatchers(nil, done)

		if _, ok := <-merged; ok {
			t.Error("expected merged to be closed immediately with no watchers")
		}
	})
}

func TestMonitorSettingsLoadsCheckOnPut(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watch(t, kv, "check.*")

	s := &Settings{Checks: map[string]Check{}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	if _, err := kv.Put("check.noop", noopCheckJSON("noop")); err != nil {
		t.Fatalf("Failed to put check: %v", err)
	}

	waitFor(t, func() bool {
		_, ok := s.GetCheck("noop")
		return ok
	})

	if c, _ := s.GetCheck("noop"); c.Name != "noop" {
		t.Errorf("Name = %q, want noop", c.Name)
	}
}

func TestMonitorSettingsBadCheckJSONIsSkipped(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watch(t, kv, "check.*")

	s := &Settings{Checks: map[string]Check{}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	if _, err := kv.Put("check.bad", []byte(`not json`)); err != nil {
		t.Fatalf("Failed to put bad check: %v", err)
	}
	// Prove forward progress: if the bad entry wedged the loop, this would never land.
	if _, err := kv.Put("check.good", noopCheckJSON("good")); err != nil {
		t.Fatalf("Failed to put good check: %v", err)
	}

	waitFor(t, func() bool {
		_, ok := s.GetCheck("good")
		return ok
	})

	if _, ok := s.GetCheck("bad"); ok {
		t.Error("bad check should not have been loaded")
	}
}

func TestMonitorSettingsRemovesCheckAfterStartup(t *testing.T) {
	kv := setupSettingsKV(t)
	if _, err := kv.Put("check.noop", noopCheckJSON("noop")); err != nil {
		t.Fatalf("Failed to seed check: %v", err)
	}
	w := watch(t, kv, "check.*")

	s := &Settings{Checks: map[string]Check{}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	waitFor(t, func() bool {
		_, ok := s.GetCheck("noop")
		return ok
	})

	if err := kv.Delete("check.noop"); err != nil {
		t.Fatalf("Failed to delete check: %v", err)
	}

	waitFor(t, func() bool {
		_, ok := s.GetCheck("noop")
		return !ok
	})
}

// TestMonitorSettingsSkipsCheckRemovalDuringStartup puts then deletes a check before
// the watcher is even created, so its replayed initial-sync state is the tombstone.
// The watcher delivers that tombstone before its own "caught up" marker, which is
// exactly the case the startUpCompleted guard exists for.
func TestMonitorSettingsSkipsCheckRemovalDuringStartup(t *testing.T) {
	kv := setupSettingsKV(t)

	if _, err := kv.Put("check.x", noopCheckJSON("x")); err != nil {
		t.Fatalf("Failed to seed check: %v", err)
	}
	if err := kv.Delete("check.x"); err != nil {
		t.Fatalf("Failed to delete check: %v", err)
	}

	w := watch(t, kv, "check.*")

	s := &Settings{Checks: map[string]Check{"x": {Name: "x"}}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	if _, err := kv.Put("check.y", noopCheckJSON("y")); err != nil {
		t.Fatalf("Failed to put check: %v", err)
	}
	waitFor(t, func() bool {
		_, ok := s.GetCheck("y")
		return ok
	})

	if _, ok := s.GetCheck("x"); !ok {
		t.Error("check x should not have been removed before startup completed")
	}
}

func TestMonitorSettingsTeamSettingsUpdate(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watch(t, kv, "0.settings")

	team := &TeamState{}
	s := &Settings{Checks: map[string]Check{}, Teams: map[uint16]*TeamState{0: team}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	payload, err := json.Marshal(map[string]map[string]string{"noop": {"key": "value"}})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := kv.Put("0.settings", payload); err != nil {
		t.Fatalf("Failed to put team settings: %v", err)
	}

	waitFor(t, func() bool {
		return team.GetAttributes("noop")["key"] == "value"
	})
}

func TestMonitorSettingsTeamSettingsRemovedAfterStartup(t *testing.T) {
	kv := setupSettingsKV(t)

	payload, err := json.Marshal(map[string]map[string]string{"noop": {"key": "value"}})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := kv.Put("0.settings", payload); err != nil {
		t.Fatalf("Failed to seed team settings: %v", err)
	}

	w := watch(t, kv, "0.settings")

	team := &TeamState{}
	s := &Settings{Checks: map[string]Check{}, Teams: map[uint16]*TeamState{0: team}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	waitFor(t, func() bool {
		return team.GetAttributes("noop")["key"] == "value"
	})

	if err := kv.Delete("0.settings"); err != nil {
		t.Fatalf("Failed to delete team settings: %v", err)
	}

	waitFor(t, func() bool {
		return team.GetAttributes("noop") == nil
	})
}

// TestMonitorSettingsTeamSettingsRemovalSkippedDuringStartup uses the same
// put-then-delete-before-watching trick as the check-removal equivalent, and adds a
// second, unrelated watcher purely as a deterministic "startup has progressed" signal.
func TestMonitorSettingsTeamSettingsRemovalSkippedDuringStartup(t *testing.T) {
	kv := setupSettingsKV(t)

	seed, err := json.Marshal(map[string]map[string]string{"noop": {"key": "value"}})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := kv.Put("0.settings", seed); err != nil {
		t.Fatalf("Failed to seed team settings: %v", err)
	}
	if err := kv.Delete("0.settings"); err != nil {
		t.Fatalf("Failed to delete team settings: %v", err)
	}

	settingsWatcher := watch(t, kv, "0.settings")
	checkWatcher := watch(t, kv, "check.*")

	team := &TeamState{Attributes: map[string]map[string]string{"noop": {"key": "value"}}}
	s := &Settings{Checks: map[string]Check{}, Teams: map[uint16]*TeamState{0: team}}
	stop := runMonitor(t, s, []nats.KeyWatcher{settingsWatcher, checkWatcher})
	defer stop()

	if _, err := kv.Put("check.y", noopCheckJSON("y")); err != nil {
		t.Fatalf("Failed to put check: %v", err)
	}
	waitFor(t, func() bool {
		_, ok := s.GetCheck("y")
		return ok
	})

	if got := team.GetAttributes("noop")["key"]; got != "value" {
		t.Errorf("team attributes were cleared before startup completed, got %q", got)
	}
}

func TestMonitorSettingsUnknownTeamKeyDoesNotPanic(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watchAll(t, kv)

	s := &Settings{Checks: map[string]Check{}, Teams: map[uint16]*TeamState{}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	if _, err := kv.Put("5.settings", []byte(`{}`)); err != nil {
		t.Fatalf("Failed to put unknown team settings: %v", err)
	}

	// Prove forward progress past the unknown team key.
	if _, err := kv.Put("check.y", noopCheckJSON("y")); err != nil {
		t.Fatalf("Failed to put check: %v", err)
	}
	waitFor(t, func() bool {
		_, ok := s.GetCheck("y")
		return ok
	})
}

func TestMonitorSettingsInvalidTeamIDDoesNotPanic(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watchAll(t, kv)

	s := &Settings{Checks: map[string]Check{}, Teams: map[uint16]*TeamState{}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	if _, err := kv.Put("notanumber.settings", []byte(`{}`)); err != nil {
		t.Fatalf("Failed to put settings with invalid team id: %v", err)
	}

	if _, err := kv.Put("check.y", noopCheckJSON("y")); err != nil {
		t.Fatalf("Failed to put check: %v", err)
	}
	waitFor(t, func() bool {
		_, ok := s.GetCheck("y")
		return ok
	})
}

func TestMonitorSettingsUnknownKeyFormatDoesNotPanic(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watchAll(t, kv)

	s := &Settings{Checks: map[string]Check{}, Teams: map[uint16]*TeamState{}}
	stop := runMonitor(t, s, []nats.KeyWatcher{w})
	defer stop()

	if _, err := kv.Put("garbage-key", []byte(`{}`)); err != nil {
		t.Fatalf("Failed to put unrecognized key: %v", err)
	}

	if _, err := kv.Put("check.y", noopCheckJSON("y")); err != nil {
		t.Fatalf("Failed to put check: %v", err)
	}
	waitFor(t, func() bool {
		_, ok := s.GetCheck("y")
		return ok
	})
}

func TestMonitorSettingsStopsOnContextCancellation(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watch(t, kv, "check.*")

	s := &Settings{Checks: map[string]Check{}}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- s.MonitorSettings(ctx, []nats.KeyWatcher{w})
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected nil error after cancellation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MonitorSettings did not stop after context cancellation")
	}
}

func TestMonitorSettingsStopsWhenWatcherChannelCloses(t *testing.T) {
	kv := setupSettingsKV(t)
	w := watch(t, kv, "check.*")

	s := &Settings{Checks: map[string]Check{}}

	done := make(chan error, 1)
	go func() {
		done <- s.MonitorSettings(context.Background(), []nats.KeyWatcher{w})
	}()

	if err := w.Stop(); err != nil {
		t.Fatalf("Failed to stop watcher: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected an error when the watcher stops while still needed, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MonitorSettings did not stop after its only watcher's channel closed")
	}
}

func TestMonitorSettingsNoWatchersReturnsImmediately(t *testing.T) {
	s := &Settings{Checks: map[string]Check{}}

	done := make(chan error, 1)
	go func() {
		done <- s.MonitorSettings(context.Background(), nil)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected nil error with no watchers, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MonitorSettings with no watchers did not return immediately")
	}
}
