package events_test

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/govengers/events"
)

// The test vector of the Standard Webhooks reference libraries.
const (
	vectorSecret    = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	vectorID        = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	vectorTimestamp = 1614265330
	vectorBody      = `{"test": 2432232314}`
	vectorSignature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func TestSignMatchesTheStandardWebhooksVector(t *testing.T) {
	header := http.Header{}
	require.NoError(t, events.Sign(header, vectorSecret, vectorID, time.Unix(vectorTimestamp, 0), []byte(vectorBody)))
	require.Equal(t, vectorSignature, header.Get(events.HeaderSignature))
	require.Equal(t, vectorID, header.Get(events.HeaderID))
	require.Equal(t, strconv.Itoa(vectorTimestamp), header.Get(events.HeaderTimestamp))
}

func vectorHeader() http.Header {
	header := http.Header{}
	header.Set(events.HeaderID, vectorID)
	header.Set(events.HeaderTimestamp, strconv.Itoa(vectorTimestamp))
	header.Set(events.HeaderSignature, vectorSignature)
	return header
}

func TestVerify(t *testing.T) {
	signedAt := time.Unix(vectorTimestamp, 0)
	t.Run("accepts the vector within the tolerance", func(t *testing.T) {
		require.NoError(t, events.Verify(vectorHeader(), vectorSecret, []byte(vectorBody), signedAt.Add(events.SignatureTolerance)))
		require.NoError(t, events.Verify(vectorHeader(), vectorSecret, []byte(vectorBody), signedAt.Add(-events.SignatureTolerance)))
	})
	t.Run("accepts any of several signatures", func(t *testing.T) {
		header := vectorHeader()
		header.Set(events.HeaderSignature, "v1,bm90IGl0 v2,ignored "+vectorSignature)
		require.NoError(t, events.Verify(header, vectorSecret, []byte(vectorBody), signedAt))
	})
	refusals := map[string]func(header http.Header) (http.Header, []byte, time.Time){
		"a changed body": func(header http.Header) (http.Header, []byte, time.Time) {
			return header, []byte(`{"test": 2432232315}`), signedAt
		},
		"a changed id": func(header http.Header) (http.Header, []byte, time.Time) {
			header.Set(events.HeaderID, "msg_other")
			return header, []byte(vectorBody), signedAt
		},
		"an old timestamp": func(header http.Header) (http.Header, []byte, time.Time) {
			return header, []byte(vectorBody), signedAt.Add(events.SignatureTolerance + time.Second)
		},
		"a future timestamp": func(header http.Header) (http.Header, []byte, time.Time) {
			return header, []byte(vectorBody), signedAt.Add(-events.SignatureTolerance - time.Second)
		},
		"a malformed timestamp": func(header http.Header) (http.Header, []byte, time.Time) {
			header.Set(events.HeaderTimestamp, "yesterday")
			return header, []byte(vectorBody), signedAt
		},
		"no headers": func(http.Header) (http.Header, []byte, time.Time) {
			return http.Header{}, []byte(vectorBody), signedAt
		},
		"another version only": func(header http.Header) (http.Header, []byte, time.Time) {
			header.Set(events.HeaderSignature, "v2,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=")
			return header, []byte(vectorBody), signedAt
		},
	}
	for name, modify := range refusals {
		t.Run("refuses "+name, func(t *testing.T) {
			header, body, now := modify(vectorHeader())
			require.ErrorIs(t, events.Verify(header, vectorSecret, body, now), events.ErrInvalidSignature)
		})
	}
	t.Run("refuses another secret", func(t *testing.T) {
		require.ErrorIs(t, events.Verify(vectorHeader(), events.NewSecret(), []byte(vectorBody), signedAt), events.ErrInvalidSignature)
	})
}

func TestSecrets(t *testing.T) {
	require.NoError(t, events.ValidateSecret(events.NewSecret()))
	require.NotEqual(t, events.NewSecret(), events.NewSecret())
	key := []byte("0123456789abcdef0123456789abcdef")
	require.Equal(t, events.DeriveSecret(key, "create-invoice"), events.DeriveSecret(key, "create-invoice"))
	require.NotEqual(t, events.DeriveSecret(key, "create-invoice"), events.DeriveSecret(key, "send-receipt"))
	require.NoError(t, events.ValidateSecret(events.DeriveSecret(key, "create-invoice")))
	for _, malformed := range []string{"", "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", "whsec_", "whsec_not base64!"} {
		require.ErrorIs(t, events.ValidateSecret(malformed), events.ErrInvalidSecret, malformed)
		require.ErrorIs(t, events.Sign(http.Header{}, malformed, "id", time.Now(), nil), events.ErrInvalidSecret, malformed)
		require.ErrorIs(t, events.Verify(vectorHeader(), malformed, nil, time.Now()), events.ErrInvalidSecret, malformed)
	}
}
