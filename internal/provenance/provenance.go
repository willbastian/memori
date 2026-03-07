package provenance

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"strings"
	"time"
)

const (
	PrincipalHuman = "human"
	PrincipalLLM   = "llm"

	EnvPrincipal            = "MEMORI_PRINCIPAL"
	EnvLLMProvider          = "MEMORI_LLM_PROVIDER"
	EnvLLMModel             = "MEMORI_LLM_MODEL"
	EnvAllowManualCommandID = "MEMORI_ALLOW_MANUAL_COMMAND_ID"

	PasswordAlgorithm         = "pbkdf2-sha256"
	DefaultPasswordIterations = 310000
	passwordSaltBytes         = 16
	passwordHashBytes         = 32
)

type Principal struct {
	Kind     string `json:"kind"`
	Actor    string `json:"actor"`
	Username string `json:"username,omitempty"`
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
}

type PasswordCredential struct {
	Algorithm  string `json:"algorithm"`
	Iterations int    `json:"iterations"`
	SaltHex    string `json:"salt_hex"`
	HashHex    string `json:"hash_hex"`
}

var (
	nowUTC     = func() time.Time { return time.Now().UTC() }
	randomRead = func(buf []byte) error {
		_, err := io.ReadFull(rand.Reader, buf)
		return err
	}
)

func ResolvePrincipal() (Principal, error) {
	return resolvePrincipal(os.Getenv, currentUsername)
}

func ResolveCommandID(operation, requested string) (string, error) {
	return resolveCommandID(operation, requested, os.Getenv)
}

func DerivePasswordCredential(password string) (PasswordCredential, error) {
	trimmed := strings.TrimSpace(password)
	if len(trimmed) < 12 {
		return PasswordCredential{}, errors.New("password must be at least 12 characters")
	}

	salt := make([]byte, passwordSaltBytes)
	if err := randomRead(salt); err != nil {
		return PasswordCredential{}, fmt.Errorf("generate password salt: %w", err)
	}

	derived := pbkdf2SHA256([]byte(trimmed), salt, DefaultPasswordIterations, passwordHashBytes)
	return PasswordCredential{
		Algorithm:  PasswordAlgorithm,
		Iterations: DefaultPasswordIterations,
		SaltHex:    hex.EncodeToString(salt),
		HashHex:    hex.EncodeToString(derived),
	}, nil
}

func VerifyPassword(password string, credential PasswordCredential) (bool, error) {
	if credential.Algorithm != PasswordAlgorithm {
		return false, fmt.Errorf("unsupported password algorithm %q", credential.Algorithm)
	}
	if credential.Iterations < DefaultPasswordIterations {
		return false, fmt.Errorf("password iterations %d below minimum %d", credential.Iterations, DefaultPasswordIterations)
	}

	salt, err := hex.DecodeString(strings.TrimSpace(credential.SaltHex))
	if err != nil {
		return false, fmt.Errorf("decode password salt: %w", err)
	}
	expected, err := hex.DecodeString(strings.TrimSpace(credential.HashHex))
	if err != nil {
		return false, fmt.Errorf("decode password hash: %w", err)
	}
	if len(expected) != passwordHashBytes {
		return false, fmt.Errorf("invalid password hash length %d", len(expected))
	}

	derived := pbkdf2SHA256([]byte(strings.TrimSpace(password)), salt, credential.Iterations, len(expected))
	return hmac.Equal(derived, expected), nil
}

func resolvePrincipal(envLookup func(string) string, usernameLookup func() (string, error)) (Principal, error) {
	kind := strings.ToLower(strings.TrimSpace(envLookup(EnvPrincipal)))
	if kind == "" {
		kind = PrincipalHuman
	}

	switch kind {
	case PrincipalHuman:
		username, err := usernameLookup()
		if err != nil {
			return Principal{}, err
		}
		token := canonicalToken(username)
		if token == "" {
			return Principal{}, errors.New("unable to derive canonical human username")
		}
		return Principal{
			Kind:     PrincipalHuman,
			Actor:    "human:" + token,
			Username: token,
		}, nil
	case PrincipalLLM:
		provider := canonicalToken(envLookup(EnvLLMProvider))
		model := canonicalToken(envLookup(EnvLLMModel))
		if provider == "" {
			return Principal{}, fmt.Errorf("%s=llm requires %s", EnvPrincipal, EnvLLMProvider)
		}
		if model == "" {
			return Principal{}, fmt.Errorf("%s=llm requires %s", EnvPrincipal, EnvLLMModel)
		}
		return Principal{
			Kind:     PrincipalLLM,
			Actor:    fmt.Sprintf("llm:%s:%s", provider, model),
			Provider: provider,
			Model:    model,
		}, nil
	default:
		return Principal{}, fmt.Errorf("invalid %s %q (expected human|llm)", EnvPrincipal, kind)
	}
}

func resolveCommandID(operation, requested string, envLookup func(string) string) (string, error) {
	manual := strings.TrimSpace(requested)
	if manual != "" {
		if !envEnabled(envLookup(EnvAllowManualCommandID)) {
			return "", fmt.Errorf("manual --command-id requires %s=1", EnvAllowManualCommandID)
		}
		return manual, nil
	}

	op := canonicalToken(operation)
	if op == "" {
		op = "mutation"
	}
	randomSuffix := make([]byte, 6)
	if err := randomRead(randomSuffix); err != nil {
		return "", fmt.Errorf("generate command id entropy: %w", err)
	}
	stamp := nowUTC().Format("20060102t150405000000000z")
	return fmt.Sprintf("cmdv1-%s-%s-%s", op, stamp, hex.EncodeToString(randomSuffix)), nil
}

func currentUsername() (string, error) {
	if current, err := user.Current(); err == nil && strings.TrimSpace(current.Username) != "" {
		return current.Username, nil
	}
	if fromEnv := strings.TrimSpace(os.Getenv("USER")); fromEnv != "" {
		return fromEnv, nil
	}
	return "", errors.New("unable to determine system username for human principal")
}

func canonicalToken(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}

	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func envEnabled(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func pbkdf2SHA256(password, salt []byte, iterations, keyLen int) []byte {
	blocks := (keyLen + sha256.Size - 1) / sha256.Size
	out := make([]byte, 0, blocks*sha256.Size)
	for block := 1; block <= blocks; block++ {
		out = append(out, pbkdf2Block(password, salt, iterations, block)...)
	}
	return out[:keyLen]
}

func pbkdf2Block(password, salt []byte, iterations, block int) []byte {
	mac := hmac.New(sha256.New, password)
	buf := make([]byte, len(salt)+4)
	copy(buf, salt)
	binary.BigEndian.PutUint32(buf[len(salt):], uint32(block))
	_, _ = mac.Write(buf)
	u := mac.Sum(nil)
	result := append([]byte(nil), u...)

	for i := 1; i < iterations; i++ {
		mac = hmac.New(sha256.New, password)
		_, _ = mac.Write(u)
		u = mac.Sum(nil)
		for j := range result {
			result[j] ^= u[j]
		}
	}
	return result
}
