package events_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/govengers/events"
)

func TestValidateTopic(t *testing.T) {
	for _, valid := range []string{"orders.order-placed.v1", "orders.v2", "billing.invoice.paid.v10"} {
		require.NoError(t, events.ValidateTopic(valid), valid)
	}
	for _, invalid := range []string{"", "orders", "orders.order-placed", "orders.order-placed.v0", "Orders.placed.v1", "orders..placed.v1", "orders.placed.v1.", "orders.-placed.v1", "v1"} {
		require.ErrorIs(t, events.ValidateTopic(invalid), events.ErrInvalidTopic, invalid)
	}
}

func TestValidateName(t *testing.T) {
	for _, valid := range []string{"billing", "create-invoice", "billing.create-invoice", "a1"} {
		require.NoError(t, events.ValidateName(valid), valid)
	}
	long := make([]byte, 101)
	for index := range long {
		long[index] = 'a'
	}
	for _, invalid := range []string{"", "Billing", "create_invoice", "create invoice", "a/b", "-a", "a-", "a..b", string(long)} {
		require.ErrorIs(t, events.ValidateName(invalid), events.ErrInvalidName, invalid)
	}
}

func TestValidateEndpoint(t *testing.T) {
	require.NoError(t, events.ValidateEndpoint("https://billing.example.com/events/create-invoice", false))
	require.NoError(t, events.ValidateEndpoint("http://billing.internal:8080/events/create-invoice", true))
	for _, invalid := range []string{
		"http://billing.example.com/events", "ftp://billing.example.com", "/events/create-invoice",
		"https://user:password@billing.example.com/events", "https://billing.example.com/events#fragment", "https://", "%zz",
	} {
		require.ErrorIs(t, events.ValidateEndpoint(invalid, false), events.ErrInvalidEndpoint, invalid)
	}
}

func TestPreparedEvent(t *testing.T) {
	topic := events.NewTopic[map[string]int]("orders.order-placed.v1")
	require.Equal(t, "orders.order-placed.v1", topic.Name())
	data, err := topic.New(map[string]int{"version": 3}).EncodeData()
	require.NoError(t, err)
	require.JSONEq(t, `{"version": 3}`, string(data))

	_, err = events.NewTopic[int]("not a topic").New(1).EncodeData()
	require.ErrorIs(t, err, events.ErrInvalidTopic)
	_, err = (&events.PreparedEvent{}).EncodeData()
	require.ErrorIs(t, err, events.ErrInvalidTopic)
	_, err = events.NewTopic[func()]("orders.callback.v1").New(func() {}).EncodeData()
	require.Error(t, err)
}
