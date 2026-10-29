package auth

import (
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/andrew-aiken/score/internal/config"
	"github.com/andrew-aiken/score/internal/logging"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

type CliParameters struct {
	ConfigFile string
	Standalone bool
	Teams      []uint16
}

func Auth(args CliParameters) error {
	logging.SetupLogging("warn")

	// Load configuration
	cfg, err := config.Load(args.ConfigFile)
	if err != nil {
		slog.Error("Failed to load config")
		return err
	}

	if args.Standalone {
		return createAgentCredentials(cfg, "*")
	}

	// Sort list so in  ascending order
	slices.Sort(args.Teams)

	for i := range args.Teams {
		team := args.Teams[i]
		fmt.Printf("\n--------- %d ---------\n", team)

		if err := createAgentCredentials(cfg, strconv.FormatUint(uint64(team), 10)); err != nil {
			return err
		}
	}

	return nil
}

func createAgentCredentials(conf config.Config, streamIndex string) error {
	signedJWT, userSeed, err := AgentCredentials(conf, streamIndex)
	if err != nil {
		return err
	}

	fmt.Printf(`
-----BEGIN NATS USER JWT-----
%s
------END NATS USER JWT------

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------
`, signedJWT, userSeed)

	return nil
}

// AgentCredentials creates a signed agent JWT and user seed scoped to a team number, or "*" for a standalone agent
func AgentCredentials(conf config.Config, streamIndex string) (signedJWT string, userSeed string, err error) {
	// Validate the account seed
	_, err = nkeys.FromSeed([]byte(conf.AccountSigningSeed))
	if err != nil {
		return "", "", fmt.Errorf("invalid account seed: %w", err)
	}

	// Create a new user keypair
	userKP, err := nkeys.CreateUser()
	if err != nil {
		return "", "", fmt.Errorf("failed to create user keypair: %w", err)
	}

	userPub, err := userKP.PublicKey()
	if err != nil {
		return "", "", fmt.Errorf("failed to get user public key: %w", err)
	}

	// Create claim permissions
	userClaim := jwt.NewUserClaims(userPub)

	userClaim.Name = fmt.Sprint("agent-" + streamIndex)

	if streamIndex == "*" {
		userClaim.Name = "agent-standalone"

		// Every watch is single-key, so the consumer-create request always carries a filter subject.
		userClaim.Pub.Allow.Add("$JS.API.CONSUMER.CREATE.KV_settings.*.>")
	} else {
		// Scope consumer creation to this team's own settings key only.
		userClaim.Pub.Allow.Add(fmt.Sprintf("$JS.API.CONSUMER.CREATE.KV_settings.*.$KV.settings.%s.settings", streamIndex))
	}

	// Every agent watches "check.*"
	userClaim.Pub.Allow.Add("$JS.API.CONSUMER.CREATE.KV_settings.*.$KV.settings.check.*")

	userClaim.IssuedAt = time.Now().Unix()
	// JWT lasts 30 days
	userClaim.Expires = time.Now().Add(30 * 24 * time.Hour).Unix()
	userClaim.IssuerAccount = conf.AccountPublicKey

	// Apply permissions
	userClaim.Pub.Allow.Add("results." + streamIndex + ".>")

	userClaim.Pub.Allow.Add("$JS.API.STREAM.INFO.KV_settings")

	// CONSUMER.DELETE cannot be scoped per-team the same way: it's authorized by stream + consumer name only
	userClaim.Pub.Allow.Add("$JS.API.CONSUMER.DELETE.KV_settings.*")

	userClaim.Sub.Allow.Add("events.score.>")
	userClaim.Sub.Allow.Add("_AGENT." + streamIndex + ".>")

	// Sign with account key
	accountKP, err := nkeys.FromSeed([]byte(conf.AccountSigningSeed))
	if err != nil {
		return "", "", fmt.Errorf("failed to create account keypair: %w", err)
	}

	signedJWT, err = userClaim.Encode(accountKP)
	if err != nil {
		return "", "", fmt.Errorf("failed to encode user JWT: %w", err)
	}

	// Get user seed
	seed, err := userKP.Seed()
	if err != nil {
		return "", "", fmt.Errorf("failed to get user seed: %w", err)
	}

	return signedJWT, string(seed), nil
}
