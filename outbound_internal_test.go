package events

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClassifyRetriesWhatARotationOrARolloutAnswers(t *testing.T) {
	cases := []struct {
		status    int
		permanent bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusNotFound, false},
		{http.StatusRequestTimeout, false},
		{http.StatusGone, true},
		{http.StatusRequestEntityTooLarge, true},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusServiceUnavailable, false},
		{http.StatusFound, true},
	}
	for _, testCase := range cases {
		result := classify(&http.Response{StatusCode: testCase.status, Header: http.Header{}})
		require.Error(t, result.err, "%d", testCase.status)
		require.Equal(t, testCase.permanent, result.permanent, "%d", testCase.status)
	}
	require.NoError(t, classify(&http.Response{StatusCode: http.StatusAccepted}).err)
}

func TestRetryAfterIsCappedWithoutOverflow(t *testing.T) {
	require.Equal(t, 30*time.Second, retryAfter("30"))
	require.Equal(t, maximumRetryAfter, retryAfter("3600"))
	require.Equal(t, maximumRetryAfter, retryAfter("9223372037"))
	require.Zero(t, retryAfter("0"))
	require.Zero(t, retryAfter("Wed, 21 Oct 2015 07:28:00 GMT"))
}

func TestHasNulCharacter(t *testing.T) {
	require.True(t, hasNulCharacter([]byte(`"a\u0000b"`)))
	require.True(t, hasNulCharacter([]byte(`"\\\u0000"`)), "an escaped backslash, then a NUL")
	require.False(t, hasNulCharacter([]byte(`"\\u0000"`)), "the text \\u0000")
	require.True(t, hasNulCharacter([]byte(`["\\u0000","\u0000"]`)), "the second one")
	require.False(t, hasNulCharacter([]byte(`"\u0001"`)))
}
