package checks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/andrew-aiken/score/internal/logging"
	"github.com/andrew-aiken/score/internal/nats"
)

func Import(natsAddress string, natsCreds string, directory string) error {
	logging.SetupLogging("warn")

	// Validate check directory
	if err := validateDirectory(directory); err != nil {
		return err
	}

	// Connect to NATS settings KV
	natsClient := nats.NatsConnection{
		NatsUrl:       natsAddress,
		NatsCredsFile: natsCreds,
	}
	err := natsClient.SetupConnection()
	if err != nil {
		slog.Warn("Failed to initialize NATS KV client")
		return err
	} else {
		slog.Debug("Connected to NATS KV bucket")
		defer natsClient.Close()
	}

	// Read files in directory
	checkFiles, err := readDirectory(directory)
	if err != nil {
		return err
	}

	err = loadKV(directory, checkFiles, &natsClient)

	return err
}

func validateDirectory(directoryPath string) error {
	info, err := os.Stat(directoryPath)

	if err == nil && info.IsDir() {
		return nil
	}

	return err
}

func readDirectory(directoryPath string) ([]string, error) {
	entries, err := os.ReadDir(directoryPath)
	if err != nil {
		return nil, err
	}

	var validFiles = []string{}

	for _, file := range entries {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".json") {
			validFiles = append(validFiles, fileName)
		}
	}

	return validFiles, nil
}

func loadKV(directory string, checkFiles []string, natsClient *nats.NatsConnection) error {
	for _, file := range checkFiles {
		checkName := fmt.Sprintf("check.%s", strings.TrimSuffix(file, ".json"))

		filePath := filepath.Join(directory, file)

		data, err := os.ReadFile(filePath) // #nosec G304 - fine with importing checks from anywhere on the system
		if err != nil {
			slog.Error("Failed to read check file", "name", file, "error", err.Error())
			return err
		}

		dst := &bytes.Buffer{}

		if err := json.Compact(dst, []byte(data)); err != nil {
			return err
		}

		_, err = natsClient.NatsKV.Put(checkName, dst.Bytes())
		if err != nil {
			return err
		}

	}

	fmt.Println("All Checks written to NATS KV")

	return nil
}
