package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// the authorization role attached to a token
//
// admin: read & write access;
// viewer: read-only access
type Role string

const (
	RoleAdmin  Role = "admin"
	RoleViewer Role = "viewer"
)

func (r Role) Allows(min Role) bool {
	// with 2 roles this isn't really that complicated. for now.
	// should really model this out for expandability's sake
	// in todo
	if r == RoleAdmin {
		return true
	}
	return r == min
}

// the authenticated principal attached to a request context
type Identity struct {
	User string
	Role Role
}

type tokenRecord struct {
	User      string `yaml:"user"`
	Role      Role   `yaml:"role"`
	TokenHash string `yaml:"token_hash"`
}

// holds parsed token records for verification
// tokens themselves are never stored, only their SHA-256 hashes
type TokenStore struct {
	records []tokenRecord
}

func LoadTokenFile(path string) (*TokenStore, error) {
	data, err := os.ReadFile(path) // #nosec G304, intended behaviour
	if err != nil {
		return nil, fmt.Errorf("reading token file %s: %w", path, err)
	}
	return ParseTokenFile(data)
}

// validates and parses token file contents (YAML):
//
//   - user: bipkis
//     role: admin
//     token_hash: "sha256:<64 hex chars>"
//   - user: bopkis
//     role: viewer
//     token_hash: "sha256:..."
func ParseTokenFile(data []byte) (*TokenStore, error) {
	var records []tokenRecord
	if err := yaml.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("parsing token file: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("token file contains no entries")
	}

	seen := make(map[string]bool, len(records))
	for i, rec := range records {
		switch {
		case strings.TrimSpace(rec.User) == "":
			return nil, fmt.Errorf("token file entry %d: user must not be empty", i+1)
		case rec.Role != RoleAdmin && rec.Role != RoleViewer:
			return nil, fmt.Errorf("token file entry %d (user %q): role must be %q or %q, got %q",
				i+1, rec.User, RoleAdmin, RoleViewer, rec.Role)
		}
		hash, err := parseTokenHash(rec.TokenHash)
		if err != nil {
			return nil, fmt.Errorf("token file entry %d (user %q): %w", i+1, rec.User, err)
		}
		if seen[hash] {
			return nil, fmt.Errorf("token file entry %d (user %q): duplicate token_hash", i+1, rec.User)
		}
		seen[hash] = true
		records[i].TokenHash = hash
	}

	return &TokenStore{records: records}, nil
}

func parseTokenHash(v string) (string, error) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "sha256:") {
		return "", fmt.Errorf("token_hash must start with \"sha256:\"")
	}
	hexPart := strings.TrimPrefix(v, "sha256:")
	if len(hexPart) != sha256.Size*2 {
		return "", fmt.Errorf("token_hash hex part must be %d characters, got %d", sha256.Size*2, len(hexPart))
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return "", fmt.Errorf("token_hash hex part is not valid hex: %w", err)
	}
	return strings.ToLower(hexPart), nil
}

// checks a presented bearer token and returns the identity it maps to
// hashes are compared in constant time to prevent timing attack
func (s *TokenStore) Verify(token string) (Identity, bool) {
	if s == nil || token == "" {
		return Identity{}, false
	}
	presented := hex.EncodeToString(sha256Sum(token))

	var id Identity
	var match int
	for _, rec := range s.records {
		equal := subtle.ConstantTimeCompare([]byte(presented), []byte(rec.TokenHash))
		match |= equal
		if equal == 1 {
			id = Identity{User: rec.User, Role: rec.Role}
		}
	}
	return id, match == 1
}

func HashToken(token string) string {
	return "sha256:" + hex.EncodeToString(sha256Sum(token))
}

// GenerateToken mints a new random bearer token of the form
// "ph_<43 base64url chars>"
func GenerateToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating token entropy: %w", err)
	}
	return "ph_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func sha256Sum(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}
