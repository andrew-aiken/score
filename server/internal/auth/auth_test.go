package auth

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func newTestAuthService(t *testing.T) *NATSAuthService {
	t.Helper()

	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("failed to create account keypair: %v", err)
	}

	seed, err := kp.Seed()
	if err != nil {
		t.Fatalf("failed to get account seed: %v", err)
	}

	pubKey, err := kp.PublicKey()
	if err != nil {
		t.Fatalf("failed to get account public key: %v", err)
	}

	svc, err := NewNATSAuthService(string(seed), pubKey)
	if err != nil {
		t.Fatalf("failed to create auth service: %v", err)
	}

	return svc
}

func generateClaims(t *testing.T, svc *NATSAuthService, team string) *UserClaims {
	t.Helper()

	creds, err := svc.GenerateCredentials("test-user", team)
	if err != nil {
		t.Fatalf("failed to generate credentials for team %q: %v", team, err)
	}

	claims, err := svc.VerifyJWT(creds.JWT)
	if err != nil {
		t.Fatalf("failed to verify generated JWT for team %q: %v", team, err)
	}

	return claims
}

func TestGenerateCredentials_Admin(t *testing.T) {
	svc := newTestAuthService(t)
	claims := generateClaims(t, svc, "admin")

	if !slices.Contains(claims.PubAllow, ">") {
		t.Errorf("expected admin PubAllow to contain \">\", got %v", claims.PubAllow)
	}
	if len(claims.PubDeny) != 0 {
		t.Errorf("expected admin PubDeny to be empty, got %v", claims.PubDeny)
	}
	if !slices.Contains(claims.SubAllow, ">") {
		t.Errorf("expected admin SubAllow to contain \">\", got %v", claims.SubAllow)
	}
}

func TestGenerateCredentials_Observer(t *testing.T) {
	svc := newTestAuthService(t)
	claims := generateClaims(t, svc, "observer")

	if !slices.Contains(claims.SubAllow, "results.>") {
		t.Errorf("expected observer SubAllow to contain \"results.>\" (read access to all teams), got %v", claims.SubAllow)
	}
	if slices.Contains(claims.PubAllow, ">") {
		t.Errorf("observer PubAllow must not contain \">\" (would grant admin-level publish), got %v", claims.PubAllow)
	}

	for _, want := range []string{
		"_INBOX.observer.>",
		"$JS.API.INFO",
		"$JS.API.STREAM.NAMES",
		"$JS.API.STREAM.INFO.results",
		"$JS.API.CONSUMER.CREATE.results.*.results.>",
		"$JS.API.CONSUMER.MSG.NEXT.results.*",
		"$JS.API.CONSUMER.DELETE.results.*",
	} {
		if !slices.Contains(claims.PubAllow, want) {
			t.Errorf("expected observer PubAllow to contain %q, got %v", want, claims.PubAllow)
		}
	}
	if len(claims.PubAllow) != 7 {
		t.Errorf("expected observer PubAllow to have exactly 7 entries (no extra publish rights), got %v", claims.PubAllow)
	}

	assertSinkConsumerDenied(t, claims)
}

func TestGenerateCredentials_Team(t *testing.T) {
	svc := newTestAuthService(t)
	claims := generateClaims(t, svc, "3")

	if !slices.Contains(claims.SubAllow, "results.3.>") {
		t.Errorf("expected team SubAllow to contain \"results.3.>\", got %v", claims.SubAllow)
	}
	if slices.Contains(claims.SubAllow, "results.>") {
		t.Errorf("expected team SubAllow to NOT contain the observer wildcard \"results.>\", got %v", claims.SubAllow)
	}
	if slices.Contains(claims.PubAllow, ">") {
		t.Errorf("expected team PubAllow to NOT contain \">\", got %v", claims.PubAllow)
	}
	if !slices.Contains(claims.PubAllow, "_INBOX.3.>") {
		t.Errorf("expected team PubAllow to contain its own inbox \"_INBOX.3.>\", got %v", claims.PubAllow)
	}
	if slices.Contains(claims.PubAllow, "_INBOX.>") {
		t.Errorf("expected team PubAllow to NOT contain every inbox \"_INBOX.>\", got %v", claims.PubAllow)
	}

	assertSinkConsumerDenied(t, claims)
}

// assertSinkConsumerDenied checks non-admin claims can't reach the server's sink consumer and hold no ack rights
func assertSinkConsumerDenied(t *testing.T, claims *UserClaims) {
	t.Helper()

	for _, want := range []string{
		"$JS.API.CONSUMER.MSG.NEXT.results.results-watcher",
		"$JS.API.CONSUMER.DELETE.results.results-watcher",
		"$JS.API.CONSUMER.CREATE.results.results-watcher",
		"$JS.API.CONSUMER.CREATE.results.results-watcher.>",
	} {
		if !slices.Contains(claims.PubDeny, want) {
			t.Errorf("expected PubDeny to contain %q, got %v", want, claims.PubDeny)
		}
	}

	for _, perm := range claims.PubAllow {
		if strings.HasPrefix(perm, "$JS.ACK") {
			t.Errorf("expected PubAllow to have no $JS.ACK subjects, got %q", perm)
		}
	}
}

// TestVerifyJWT_RejectsForgedIssuer demonstrates that VerifyJWT must reject a token that is NOT signed by the trusted account key
func TestVerifyJWT_RejectsForgedIssuer(t *testing.T) {
	svc := newTestAuthService(t)

	attackerKP, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("failed to create attacker keypair: %v", err)
	}

	userKP, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("failed to create user keypair: %v", err)
	}
	userPub, err := userKP.PublicKey()
	if err != nil {
		t.Fatalf("failed to get user public key: %v", err)
	}

	forged := jwt.NewUserClaims(userPub)
	forged.Name = "admin"
	forged.IssuedAt = time.Now().Unix()
	forged.Expires = time.Now().Add(time.Hour).Unix()

	// Known nats server public key
	forged.IssuerAccount = svc.accountPubKey
	forged.Tags.Add("user_id:attacker")
	forged.Tags.Add("team_id:admin")
	forged.Pub.Allow.Add(">")
	forged.Sub.Allow.Add(">")

	// Signed with the attacker's own throwaway key, NOT svc's account seed.
	forgedJWT, err := forged.Encode(attackerKP)
	if err != nil {
		t.Fatalf("failed to encode forged JWT: %v", err)
	}

	claims, err := svc.VerifyJWT(forgedJWT)
	if err == nil {
		t.Fatalf("VerifyJWT accepted a token not signed by the trusted account key (forged issuer_account field); got claims=%+v", claims)
	}
}
