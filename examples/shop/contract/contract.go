// Package contract is the orders service's published contract: the topics and their data.
// Subscribers import it; it depends on nothing beyond govengers/events (E2).
package contract

import "github.com/govengers/events"

// OrderPlaced is the data of OrderPlacedTopic. Version lets a subscriber drop a stale update
// (E10): deliveries are unordered.
type OrderPlaced struct {
	OrderID string `json:"orderId"`
	Total   int    `json:"total"` // in cents
	Version int    `json:"version"`
}

// OrderPlacedTopic is published once per placed order.
var OrderPlacedTopic = events.NewTopic[OrderPlaced]("orders.order-placed.v1")
