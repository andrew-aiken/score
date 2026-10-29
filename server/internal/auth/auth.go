package auth

import (
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// resultsSinkConsumer is the server's durable consumer on the results stream
// (created in cmd/initialize/initialize.go, consumed by internal/sink/consumer.go)
const resultsSinkConsumer = "results-watcher"

// Credentials holds the NATS JWT and seed for authentication
type Credentials struct {
	JWT  string `json:"jwt"`
	Seed string `json:"seed"`
}

// UserClaims represents the extracted claims from a NATS JWT
type UserClaims struct {
	UserID        string
	TeamID        string
	PubAllow      []string
	PubDeny       []string
	SubAllow      []string
	ExpiresAt     time.Time
	IssuedAt      time.Time
	IssuerAccount string
}

// NATSAuthService handles NATS JWT operations
type NATSAuthService struct {
	accountSeed   []byte
	accountPubKey string
	signingPubKey string
}

// NewNATSAuthService creates a new NATS auth service
func NewNATSAuthService(accountSeed, accountPubKey string) (*NATSAuthService, error) {
	kp, err := nkeys.FromSeed([]byte(accountSeed))
	if err != nil {
		return nil, fmt.Errorf("invalid account seed: %w", err)
	}

	signingPubKey, err := kp.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get signing public key: %w", err)
	}

	return &NATSAuthService{
		accountSeed:   []byte(accountSeed),
		accountPubKey: accountPubKey,
		signingPubKey: signingPubKey,
	}, nil
}

// GenerateCredentials creates NATS credentials for a user with team-based permissions
func (s *NATSAuthService) GenerateCredentials(userID string, team string) (*Credentials, error) {
	// Create a new user keypair
	userKP, err := nkeys.CreateUser()
	if err != nil {
		return nil, fmt.Errorf("failed to create user keypair: %w", err)
	}

	userPub, err := userKP.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get user public key: %w", err)
	}

	// Create user claims
	userClaim := jwt.NewUserClaims(userPub)
	userClaim.Name = team
	userClaim.IssuedAt = time.Now().Unix()
	userClaim.Expires = time.Now().Add(24 * time.Hour).Unix()
	userClaim.IssuerAccount = s.accountPubKey

	// Store user metadata in tags
	userClaim.Tags.Add(fmt.Sprintf("user_id:%s", userID))
	userClaim.Tags.Add(fmt.Sprintf("team_id:%s", team))

	// Apply team-based permissions
	s.applyPermissions(userClaim, team)

	// Sign with account key
	accountKP, err := nkeys.FromSeed(s.accountSeed)
	if err != nil {
		return nil, fmt.Errorf("failed to create account keypair: %w", err)
	}

	signedJWT, err := userClaim.Encode(accountKP)
	if err != nil {
		return nil, fmt.Errorf("failed to encode user JWT: %w", err)
	}

	// Get user seed
	userSeed, err := userKP.Seed()
	if err != nil {
		return nil, fmt.Errorf("failed to get user seed: %w", err)
	}

	return &Credentials{
		JWT:  signedJWT,
		Seed: string(userSeed),
	}, nil
}

// applyPermissions sets NATS pub/sub permissions based on the user's team
func (s *NATSAuthService) applyPermissions(userClaim *jwt.UserClaims, team string) {
	teamSubject := "results." + team + ".>"

	switch team {
	case "admin":
		userClaim.Pub.Allow.Add(">")
		userClaim.Sub.Allow.Add(">")
		return
	case "observer":
		teamSubject = "results.>"
	}

	userClaim.Pub.Allow.Add("_INBOX." + team + ".>")
	userClaim.Pub.Allow.Add("$JS.API.INFO")
	userClaim.Pub.Allow.Add("$JS.API.STREAM.NAMES")
	userClaim.Pub.Allow.Add("$JS.API.STREAM.INFO.results")
	userClaim.Pub.Allow.Add("$JS.API.CONSUMER.CREATE.results.*." + teamSubject)
	userClaim.Pub.Allow.Add("$JS.API.CONSUMER.MSG.NEXT.results.*")
	userClaim.Pub.Allow.Add("$JS.API.CONSUMER.DELETE.results.*")

	// The consumer wildcards above would otherwise match the server's sink consumer
	userClaim.Pub.Deny.Add("$JS.API.CONSUMER.MSG.NEXT.results." + resultsSinkConsumer)
	userClaim.Pub.Deny.Add("$JS.API.CONSUMER.DELETE.results." + resultsSinkConsumer)
	userClaim.Pub.Deny.Add("$JS.API.CONSUMER.CREATE.results." + resultsSinkConsumer)
	userClaim.Pub.Deny.Add("$JS.API.CONSUMER.CREATE.results." + resultsSinkConsumer + ".>")

	userClaim.Sub.Allow.Add(teamSubject)
	userClaim.Sub.Allow.Add("_INBOX." + team + ".>")
}

// VerifyJWT validates a NATS JWT and returns the user claims
func (s *NATSAuthService) VerifyJWT(jwtString string) (*UserClaims, error) {
	// Decode the JWT
	claim, err := jwt.DecodeUserClaims(jwtString)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT: %w", err)
	}

	// Verify the issuer matches the signing key this server uses to sign JWTs
	if claim.Issuer != s.signingPubKey {
		return nil, fmt.Errorf("invalid issuer")
	}

	// Verify the issuer account matches
	if claim.IssuerAccount != s.accountPubKey {
		return nil, fmt.Errorf("invalid issuer account")
	}

	// Validate the claims (checks expiry, etc.)
	vr := jwt.CreateValidationResults()
	claim.Validate(vr)
	if vr.IsBlocking(true) {
		return nil, fmt.Errorf("JWT validation failed: %v", vr.Errors())
	}

	// Extract user ID from tags
	var userID string
	for _, tag := range claim.Tags {
		if len(tag) > 8 && tag[:8] == "user_id:" {
			userID = tag[8:]
		}
	}

	// Extract permissions
	var pubAllow, pubDeny, subAllow []string
	for _, perm := range claim.Pub.Allow {
		pubAllow = append(pubAllow, perm)
	}
	for _, perm := range claim.Pub.Deny {
		pubDeny = append(pubDeny, perm)
	}
	for _, perm := range claim.Sub.Allow {
		subAllow = append(subAllow, perm)
	}

	return &UserClaims{
		UserID:        userID,
		TeamID:        claim.Name,
		PubAllow:      pubAllow,
		PubDeny:       pubDeny,
		SubAllow:      subAllow,
		ExpiresAt:     time.Unix(claim.Expires, 0),
		IssuedAt:      time.Unix(claim.IssuedAt, 0),
		IssuerAccount: claim.IssuerAccount,
	}, nil
}
