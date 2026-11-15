package events

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The headers of Standard Webhooks (https://www.standardwebhooks.com), which every delivery carries.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

// SignatureTolerance is how far a delivery's timestamp may lie from the receiver's clock, in
// either direction, before Verify refuses it as a replay.
const SignatureTolerance = 5 * time.Minute

// secretPrefix marks a Standard Webhooks secret; the base64 after it is the HMAC key.
const secretPrefix = "whsec_"

// secretBytes is the length of the keys NewSecret and DeriveSecret produce.
const secretBytes = 32

var (
	// ErrInvalidSecret is returned for a secret that is not "whsec_" followed by base64.
	ErrInvalidSecret = errors.New("events: invalid secret")
	// ErrInvalidSignature is returned by Verify for a delivery whose headers are missing, whose
	// timestamp is outside SignatureTolerance or whose signatures all fail.
	ErrInvalidSignature = errors.New("events: invalid signature")
)

// NewSecret returns a random signing secret in the Standard Webhooks format, for a subscription
// the application creates itself (a webhook a user configures).
func NewSecret() string {
	key := make([]byte, secretBytes)
	_, _ = rand.Read(key) // crypto/rand.Read never fails (Go 1.24+)
	return encodeSecret(key)
}

// DeriveSecret returns the signing secret of a subscription from the subscriber's inbox key
// (E8): HMAC-SHA256(inboxKey, subscription). The subscriber sends it at registration and stores
// nothing; rotating the inbox key and registering again rotates every secret.
func DeriveSecret(inboxKey []byte, subscription string) string {
	mac := hmac.New(sha256.New, inboxKey)
	mac.Write([]byte(subscription))
	return encodeSecret(mac.Sum(nil))
}

func encodeSecret(key []byte) string {
	return secretPrefix + base64.StdEncoding.EncodeToString(key)
}

func decodeSecret(secret string) ([]byte, error) {
	encoded, found := strings.CutPrefix(secret, secretPrefix)
	if !found {
		return nil, fmt.Errorf("%w: missing %s prefix", ErrInvalidSecret, secretPrefix)
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(key) == 0 {
		return nil, fmt.Errorf("%w: the key is not base64", ErrInvalidSecret)
	}
	return key, nil
}

// ValidateSecret returns ErrInvalidSecret unless secret is in the Standard Webhooks format.
func ValidateSecret(secret string) error {
	_, err := decodeSecret(secret)
	return err
}

// Sign sets the Standard Webhooks headers of a delivery of body: the message ID, the timestamp
// and the HMAC-SHA256 signature over "id.timestamp.body". It returns ErrInvalidSecret for a
// malformed secret.
func Sign(header http.Header, secret, id string, timestamp time.Time, body []byte) error {
	key, err := decodeSecret(secret)
	if err != nil {
		return err
	}
	unix := strconv.FormatInt(timestamp.Unix(), 10)
	header.Set(HeaderID, id)
	header.Set(HeaderTimestamp, unix)
	header.Set(HeaderSignature, "v1,"+signature(key, id, unix, body))
	return nil
}

// Verify checks a delivery's Standard Webhooks headers against secret on the raw body, before
// anything parses it. It accepts any of the space-separated v1 signatures, so a sender may sign
// with an old and a new secret during a rotation, and refuses a timestamp more than
// SignatureTolerance away from now. It returns ErrInvalidSignature, or ErrInvalidSecret for a
// malformed secret.
func Verify(header http.Header, secret string, body []byte, now time.Time) error {
	key, err := decodeSecret(secret)
	if err != nil {
		return err
	}
	id, unix := header.Get(HeaderID), header.Get(HeaderTimestamp)
	if id == "" || unix == "" {
		return fmt.Errorf("%w: missing %s or %s", ErrInvalidSignature, HeaderID, HeaderTimestamp)
	}
	seconds, err := strconv.ParseInt(unix, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: malformed timestamp", ErrInvalidSignature)
	}
	if age := now.Sub(time.Unix(seconds, 0)); age > SignatureTolerance || age < -SignatureTolerance {
		return fmt.Errorf("%w: timestamp outside the tolerance", ErrInvalidSignature)
	}
	expected := []byte(signature(key, id, unix, body))
	for candidate := range strings.FieldsSeq(header.Get(HeaderSignature)) {
		version, value, found := strings.Cut(candidate, ",")
		if found && version == "v1" && hmac.Equal([]byte(value), expected) {
			return nil
		}
	}
	return fmt.Errorf("%w: no signature matches", ErrInvalidSignature)
}

func signature(key []byte, id, unix string, body []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + unix + "."))
	mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
