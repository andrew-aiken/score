package score

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/andrew-aiken/score/internal/settings"

	"github.com/andrew-aiken/checks"

	"github.com/nats-io/nats.go"
)

// HandleScoreEvent returns a handler for nats that triggers whenever a scoring event is triggered
// Each event runs in its own goroutine so a slow check never delays other checks
func HandleScoreEvent(ctx context.Context, settings *settings.Settings, js nats.JetStreamContext, inflight *sync.WaitGroup) nats.MsgHandler {
	return func(msg *nats.Msg) {
		checkName := strings.TrimPrefix(msg.Subject, "events.score.")

		inflight.Go(func() {
			runScoreEvent(ctx, settings, js, checkName)
		})
	}
}

// runScoreEvent runs a check for every team and waits for all of them to finish
func runScoreEvent(ctx context.Context, settings *settings.Settings, js nats.JetStreamContext, checkName string) {
	value, ok := settings.GetCheck(checkName)
	if !ok {
		slog.Error(fmt.Sprintf("Check %s not found", checkName))
		return
	}

	// Verify the definition implements Checker before fanning out
	if _, ok := value.Definition.(checks.Checker); !ok {
		slog.Error(fmt.Sprintf("Check %s definition does not implement Checker interface", checkName))
		return
	}

	// Marshal the definition once; each goroutine unmarshals into its own struct.
	defBytes, err := json.Marshal(value.Definition)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to marshal definition for check %s", checkName), "error", err.Error())
		return
	}

	start := time.Now()

	var wg sync.WaitGroup
	for teamNum, teamState := range settings.Teams {
		wg.Go(func() {
			runTeamCheck(ctx, teamNum, teamState, checkName, value, defBytes, js)
		})
	}
	wg.Wait()

	// Overlapping runs are allowed, so flag checks that can't keep up with their schedule
	frequency := time.Duration(value.Frequency) * time.Second
	if elapsed := time.Since(start); frequency > 0 && elapsed > frequency {
		slog.Warn("Check run exceeded its frequency", "check", checkName, "duration", elapsed, "frequency", frequency)
	}
}

// runTeamCheck triggers a score check for a single team
func runTeamCheck(ctx context.Context, teamNum uint16, teamState *settings.TeamState, checkName string, value settings.Check, defBytes []byte, js nats.JetStreamContext) {
	defCopy := reflect.New(reflect.TypeOf(value.Definition).Elem()).Interface()
	if err := json.Unmarshal(defBytes, defCopy); err != nil {
		slog.Error(fmt.Sprintf("Failed to unmarshal definition copy for check %s team %d", checkName, teamNum), "error", err.Error())
		return
	}

	checker := defCopy.(checks.Checker)

	// Apply team-specific attribute overrides
	override := allowedArgumentOverrides(value.MutableFields, teamState.GetAttributes(checkName))
	if err := applyOverrides(defCopy, override); err != nil {
		slog.Warn(fmt.Sprintf("Failed to apply overrides for check %s team %d: %v", checkName, teamNum, err))
	}

	slog.Debug(fmt.Sprintf("Starting check: %s for team %d", checkName, teamNum))

	result := checker.Run(ctx, teamState.StaticConf)

	streamName := fmt.Sprintf("results.%d.%s", teamNum, checkName)
	if err := publishResults(streamName, result, value.ScoreWeight, js); err != nil {
		slog.Error(fmt.Sprintf("Failed to publish results for team %d", teamNum), "error", err.Error())
	}

	slog.Info(fmt.Sprintf("Check %s team %d result: %v", checkName, teamNum, result.Passed))
}

func allowedArgumentOverrides(allowedItems []string, attributes map[string]string) map[string]string {
	override := make(map[string]string)
	for key, value := range attributes {
		for _, allowed := range allowedItems {
			if key == allowed {
				override[key] = cleanTemplateString(value)
			}
		}
	}
	return override
}

// There is probably a better way to do this but for now just strip the {{ }} from the string
func cleanTemplateString(definition string) string {
	return strings.ReplaceAll(strings.ReplaceAll(definition, "{{", ""), "}}", "")
}

// applyOverrides uses reflection to set field values on the definition struct
func applyOverrides(definition any, overrides map[string]string) error {
	if len(overrides) == 0 {
		return nil
	}

	val := reflect.ValueOf(definition)
	if val.Kind() != reflect.Pointer || val.IsNil() {
		return fmt.Errorf("definition must be a non-nil pointer")
	}

	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("definition must point to a struct")
	}

	typ := val.Type()

	for key, value := range overrides {
		// Find the field by name (case-insensitive match)
		var field reflect.Value
		var found bool
		for i := 0; i < typ.NumField(); i++ {
			if strings.EqualFold(typ.Field(i).Name, key) {
				field = val.Field(i)
				found = true
				break
			}
		}

		if !found {
			slog.Warn(fmt.Sprintf("Field %s not found in definition", key))
			continue
		}

		if !field.CanSet() {
			slog.Warn(fmt.Sprintf("Field %s cannot be set", key))
			continue
		}

		// Convert string value to appropriate type
		switch field.Kind() {
		case reflect.String:
			field.SetString(value)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			intVal, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse int value for field %s: %w", key, err)
			}
			field.SetInt(intVal)
		case reflect.Bool:
			boolVal, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("failed to parse bool value for field %s: %w", key, err)
			}
			field.SetBool(boolVal)
		case reflect.Float32, reflect.Float64:
			floatVal, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return fmt.Errorf("failed to parse float value for field %s: %w", key, err)
			}
			field.SetFloat(floatVal)
		default:
			return fmt.Errorf("unsupported field type %s for field %s", field.Kind(), key)
		}
	}

	return nil
}
