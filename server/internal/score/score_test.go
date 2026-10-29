package score

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andrew-aiken/score/internal/settings"

	"github.com/andrew-aiken/checks"
	"github.com/andrew-aiken/checks/noop"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// syncBuffer is safe for the concurrent writes (from NATS callback goroutines)
// and reads (from the polling test goroutine) that captureLogs enables.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// waitForLog polls buf until it contains substr or timeout elapses, returning
// the buffer's contents either way. Handler goroutines run async relative to
// nc.Publish, so asserting on logs.String() immediately after publishing is a race.
func waitForLog(buf *syncBuffer, substr string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for {
		got := buf.String()
		if strings.Contains(got, substr) || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandleScoreEvent(t *testing.T) {
	scoreSubject := "events.score.noop"

	opts := natsserver.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()

	server := natsserver.RunServer(&opts)
	defer server.Shutdown()

	nc, err := nats.Connect(server.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Failed to connect to JetStream: %v", err)
	}

	noopCheck := settings.Check{
		Name:          "noop",
		Type:          "noop",
		ScoreWeight:   1,
		MutableFields: []string{},
		Definition: &noop.Definition{
			Pass: true,
		},
	}

	team := settings.TeamState{
		StaticConf: checks.StaticConf{
			TeamNumber: 0,
		},
		Attributes: map[string]map[string]string{
			"noop": {
				"pass": "true",
			},
		},
	}

	// Tests if the nats result stream does not exist
	t.Run("MissingSubject", func(t *testing.T) {
		logs := captureLogs(t)

		settings := settings.Settings{
			Checks: map[string]settings.Check{
				"noop": noopCheck,
			},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
			},
		}

		subscribeScoreEvents(t, nc, js, scoreSubject, &settings)

		err = nc.Publish(scoreSubject, []byte(""))
		if err != nil {
			t.Fatalf("failed to publish score trigger %v", err)
		}

		expectedError := `"Failed publish to results" stream=results.0.noop`
		got := waitForLog(logs, expectedError, 1*time.Second)
		if !strings.Contains(got, expectedError) {
			t.Errorf("expected \"%s\" to be logged, got:\n%s", expectedError, got)
		}
	})

	resultsStream := nats.StreamConfig{
		Name:        "results",
		Description: "Stream of score update events",
		Subjects:    []string{"results.>"},
		MaxAge:      30 * 24 * time.Hour, // 30 days
		Replicas:    1,
		MaxMsgs:     -1,
		MaxBytes:    -1,
		MaxMsgSize:  -1,
		DenyDelete:  true,
		DenyPurge:   true,
		AllowRollup: false,
		Duplicates:  2 * time.Minute,
	}

	_, err = js.AddStream(&resultsStream)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Valid", func(t *testing.T) {
		logs := captureLogs(t)

		settings := settings.Settings{
			Checks: map[string]settings.Check{
				"noop": noopCheck,
			},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
				1: &team,
			},
		}

		subscribeScoreEvents(t, nc, js, scoreSubject, &settings)

		err = nc.Publish(scoreSubject, []byte(""))
		if err != nil {
			t.Fatalf("failed to publish score trigger %v", err)
		}

		got := waitForLog(logs, "DNE", 1*time.Second)
		if strings.Contains(got, "ERROR") {
			t.Errorf("Expected no errors:\n%s", got)
		}

		// if strings.Contains(logs.String(), "ERROR") {
		// 	t.Errorf("unexpected error logged:\n%s", logs.String())
		// }
	})

	// Tests if the Settings object contains the check subject triggered in nats
	t.Run("MissingCheck", func(t *testing.T) {
		logs := captureLogs(t)

		settings := settings.Settings{
			Checks: map[string]settings.Check{},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
			},
		}

		subscribeScoreEvents(t, nc, js, scoreSubject, &settings)

		err = nc.Publish(scoreSubject, []byte(""))
		if err != nil {
			t.Fatalf("failed to publish score trigger %v", err)
		}

		expectedError := "Check noop not found"
		got := waitForLog(logs, expectedError, 1*time.Second)
		if !strings.Contains(got, expectedError) {
			t.Errorf("expected \"%s\" to be logged, got:\n%s", expectedError, got)
		}
	})

	// Tests if the checker object contains its interface
	t.Run("NoCheckerInterface", func(t *testing.T) {
		logs := captureLogs(t)

		settings := settings.Settings{
			Checks: map[string]settings.Check{
				"noop": settings.Check{
					Name:        "noop",
					Type:        "noop",
					ScoreWeight: 1,
					Definition:  "any-not-def",
				},
			},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
			},
		}

		subscribeScoreEvents(t, nc, js, scoreSubject, &settings)

		err = nc.Publish(scoreSubject, []byte(""))
		if err != nil {
			t.Fatalf("failed to publish score trigger %v", err)
		}

		expectedError := "Check noop definition does not implement Checker interface"
		got := waitForLog(logs, expectedError, 1*time.Second)
		if !strings.Contains(got, expectedError) {
			t.Errorf("expected \"%s\" to be logged, got:\n%s", expectedError, got)
		}
	})

	// Tests if the check definition has unmarshalable json objects
	t.Run("Unmarshalable", func(t *testing.T) {
		logs := captureLogs(t)

		settings := settings.Settings{
			Checks: map[string]settings.Check{
				"noop": {
					Name:        "noop",
					Type:        "noop",
					ScoreWeight: 1,
					MutableFields: []string{
						"pass",
					},
					Definition: &foo{},
				},
			},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
			},
		}

		subscribeScoreEvents(t, nc, js, scoreSubject, &settings)

		err = nc.Publish(scoreSubject, []byte(""))
		if err != nil {
			t.Fatalf("failed to publish score trigger %v", err)
		}

		expectedError := `"Failed to marshal definition for check noop" error="json: unsupported type: chan int"`
		got := waitForLog(logs, expectedError, 1*time.Second)
		if !strings.Contains(got, expectedError) {
			t.Errorf("expected \"%s\" to be logged, got:\n%s", expectedError, got)
		}
	})

	// A slow check must not delay other checks triggered on the same subscription
	t.Run("SlowCheckDoesNotBlock", func(t *testing.T) {
		settings := settings.Settings{
			Checks: map[string]settings.Check{
				"slow": {Name: "slow", ScoreWeight: 1, Definition: &sleepDefinition{Millis: 2000}},
				"noop": noopCheck,
			},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
			},
		}

		results, err := nc.SubscribeSync("results.0.>")
		if err != nil {
			t.Fatalf("failed to subscribe to results %v", err)
		}
		t.Cleanup(func() { _ = results.Unsubscribe() })

		subscribeScoreEvents(t, nc, js, "events.score.>", &settings)

		for _, subject := range []string{"events.score.slow", "events.score.noop"} {
			if err := nc.Publish(subject, []byte("")); err != nil {
				t.Fatalf("failed to publish score trigger %v", err)
			}
		}

		if got := nextResult(t, results, time.Second); got != "results.0.noop.0" {
			t.Fatalf("expected the noop result while the slow check runs, got %q", got)
		}
		if got := nextResult(t, results, 3*time.Second); got != "results.0.slow.0" {
			t.Fatalf("expected the slow result, got %q", got)
		}
	})

	// Every trigger runs, even while an earlier run of the same check is still in progress
	t.Run("OverlappingTriggersAllRun", func(t *testing.T) {
		settings := settings.Settings{
			Checks: map[string]settings.Check{
				"overlap": {Name: "overlap", ScoreWeight: 1, Definition: &sleepDefinition{Millis: 500}},
			},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
			},
		}

		results, err := nc.SubscribeSync("results.0.overlap.>")
		if err != nil {
			t.Fatalf("failed to subscribe to results %v", err)
		}
		t.Cleanup(func() { _ = results.Unsubscribe() })

		subscribeScoreEvents(t, nc, js, "events.score.overlap", &settings)

		for range 2 {
			if err := nc.Publish("events.score.overlap", []byte("")); err != nil {
				t.Fatalf("failed to publish score trigger %v", err)
			}
		}

		for range 2 {
			if got := nextResult(t, results, 2*time.Second); got != "results.0.overlap.0" {
				t.Fatalf("expected an overlap result, got %q", got)
			}
		}
	})

	// A check that takes longer than its frequency is flagged
	t.Run("SlowRunWarns", func(t *testing.T) {
		logs := captureLogs(t)

		settings := settings.Settings{
			Checks: map[string]settings.Check{
				"sleepy": {Name: "sleepy", ScoreWeight: 1, Frequency: 1, Definition: &sleepDefinition{Millis: 1100}},
			},
			Teams: map[uint16]*settings.TeamState{
				0: &team,
			},
		}

		subscribeScoreEvents(t, nc, js, "events.score.sleepy", &settings)

		if err := nc.Publish("events.score.sleepy", []byte("")); err != nil {
			t.Fatalf("failed to publish score trigger %v", err)
		}

		expected := `"Check run exceeded its frequency" check=sleepy`
		got := waitForLog(logs, expected, 3*time.Second)
		if !strings.Contains(got, expected) {
			t.Errorf("expected \"%s\" to be logged, got:\n%s", expected, got)
		}
	})
}

// subscribeScoreEvents subscribes HandleScoreEvent to subject, cleanup unsubscribes and then waits for running checks
func subscribeScoreEvents(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, subject string, settings *settings.Settings) {
	t.Helper()

	var inflight sync.WaitGroup
	t.Cleanup(inflight.Wait)

	sub, err := nc.Subscribe(subject, HandleScoreEvent(context.Background(), settings, js, &inflight))
	if err != nil {
		t.Fatalf("failed to subscribe to stream %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

// nextResult returns the subject of the next published result, failing if none arrives within timeout
func nextResult(t *testing.T, sub *nats.Subscription, timeout time.Duration) string {
	t.Helper()

	msg, err := sub.NextMsg(timeout)
	if err != nil {
		t.Fatalf("no result published: %v", err)
	}

	return msg.Subject
}

func TestCleanTemplateString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"{{.TeamNumber}}", ".TeamNumber"},
		{"{{.TeamNumberHex}}.example.com", ".TeamNumberHex.example.com"},
		{"plain", "plain"},
		{"no braces", "no braces"},
		{"{{nested{{double}}", "nesteddouble"},
	}
	for _, tt := range tests {
		got := cleanTemplateString(tt.input)
		if got != tt.want {
			t.Errorf("cleanTemplateString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestAllowedArgumentOverrides(t *testing.T) {
	t.Run("allows only permitted fields", func(t *testing.T) {
		allowed := []string{"host", "port"}
		attrs := map[string]string{
			"host":   "{{.TeamNumberHex}}.example.com",
			"port":   "8080",
			"secret": "password",
		}

		result := allowedArgumentOverrides(allowed, attrs)

		if _, ok := result["secret"]; ok {
			t.Error("secret should not be in result")
		}
		if result["host"] != ".TeamNumberHex.example.com" {
			t.Errorf("host = %q, want cleaned template", result["host"])
		}
		if result["port"] != "8080" {
			t.Errorf("port = %q, want 8080", result["port"])
		}
	})

	t.Run("nil attributes produces empty map", func(t *testing.T) {
		result := allowedArgumentOverrides([]string{"host"}, nil)
		if len(result) != 0 {
			t.Errorf("expected empty map, got %v", result)
		}
	})

	t.Run("empty allowed list produces empty map", func(t *testing.T) {
		result := allowedArgumentOverrides(nil, map[string]string{"host": "x"})
		if len(result) != 0 {
			t.Errorf("expected empty map, got %v", result)
		}
	})
}

func TestApplyOverrides(t *testing.T) {
	type target struct {
		Host    string
		Port    int
		Enabled bool
		Score   float64
		Object  struct{}
	}

	t.Run("string field", func(t *testing.T) {
		s := &target{Host: "original"}
		if err := applyOverrides(s, map[string]string{"Host": "new-host"}); err != nil {
			t.Fatal(err)
		}
		if s.Host != "new-host" {
			t.Errorf("Host = %q, want %q", s.Host, "new-host")
		}
	})

	t.Run("int field", func(t *testing.T) {
		s := &target{}
		if err := applyOverrides(s, map[string]string{"Port": "9090"}); err != nil {
			t.Fatal(err)
		}
		if s.Port != 9090 {
			t.Errorf("Port = %d, want 9090", s.Port)
		}
	})

	t.Run("bool field", func(t *testing.T) {
		s := &target{}
		if err := applyOverrides(s, map[string]string{"Enabled": "true"}); err != nil {
			t.Fatal(err)
		}
		if !s.Enabled {
			t.Error("Enabled should be true")
		}
	})

	t.Run("float field", func(t *testing.T) {
		s := &target{}
		if err := applyOverrides(s, map[string]string{"Score": "3.14"}); err != nil {
			t.Fatal(err)
		}
		if s.Score != 3.14 {
			t.Errorf("Score = %f, want 3.14", s.Score)
		}
	})

	t.Run("case insensitive match", func(t *testing.T) {
		s := &target{}
		if err := applyOverrides(s, map[string]string{"host": "lower"}); err != nil {
			t.Fatal(err)
		}
		if s.Host != "lower" {
			t.Errorf("Host = %q, want %q", s.Host, "lower")
		}
	})

	t.Run("empty overrides is no-op", func(t *testing.T) {
		s := &target{Host: "original"}
		if err := applyOverrides(s, map[string]string{}); err != nil {
			t.Fatal(err)
		}
		if s.Host != "original" {
			t.Error("Host should not have changed")
		}
	})

	t.Run("nil pointer returns error", func(t *testing.T) {
		var s *target
		if err := applyOverrides(s, map[string]string{"Host": "x"}); err == nil {
			t.Error("expected error for nil pointer")
		}
	})

	t.Run("non-pointer returns error", func(t *testing.T) {
		s := target{}
		if err := applyOverrides(s, map[string]string{"Host": "x"}); err == nil {
			t.Error("expected error for non-pointer")
		}
	})

	t.Run("invalid int returns error", func(t *testing.T) {
		s := &target{}
		if err := applyOverrides(s, map[string]string{"Port": "not-a-number"}); err == nil {
			t.Error("expected error for invalid int")
		}
	})

	t.Run("unknown field is skipped", func(t *testing.T) {
		s := &target{Host: "original"}
		if err := applyOverrides(s, map[string]string{"NonExistent": "x"}); err != nil {
			t.Fatal(err)
		}
		if s.Host != "original" {
			t.Error("Host should not have changed")
		}
	})

	t.Run("bool field not a bool", func(t *testing.T) {
		s := &target{}
		err := applyOverrides(s, map[string]string{"Enabled": "not-bool"})

		if !strings.Contains(err.Error(), "failed to parse bool value for field") {
			t.Fatal("Got wrong error when parsing incorrect boolean value")
		}
	})

	t.Run("float field not a float", func(t *testing.T) {
		s := &target{}
		err := applyOverrides(s, map[string]string{"Score": "string"})

		if !strings.Contains(err.Error(), "failed to parse float value for field Score") {
			t.Fatal("Got wrong error when parsing incorrect float value")
		}
	})

	t.Run("unsupported field", func(t *testing.T) {
		s := &target{}
		err := applyOverrides(s, map[string]string{"Object": "string"})

		if !strings.Contains(err.Error(), "unsupported field type struct for field Object") {
			t.Fatalf("Got wrong error when parsing incorrect float value: %s", err.Error())
		}
	})
}

// foo is a dummy method that implements the expected Check structure
type foo struct {
	Pass chan int
}

func (foo) Run(ctx context.Context, static checks.StaticConf) checks.Results {
	return checks.Results{}
}

func (foo) Validate() (passed bool, message string) {
	return true, ""
}

// sleepDefinition is a check that passes after sleeping for Millis milliseconds
type sleepDefinition struct {
	Millis int
}

func (d sleepDefinition) Run(ctx context.Context, static checks.StaticConf) checks.Results {
	time.Sleep(time.Duration(d.Millis) * time.Millisecond)
	return checks.Results{Passed: true, Details: map[string]string{}}
}

func (sleepDefinition) Validate() (passed bool, message string) {
	return true, ""
}
