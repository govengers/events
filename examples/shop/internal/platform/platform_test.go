package platform

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveringTurnsAPanicIntoAnError(t *testing.T) {
	err := Recovering(func() error { panic("handler bug") })()
	require.ErrorContains(t, err, "panic: handler bug")
	require.NoError(t, Recovering(func() error { return nil })())
}
