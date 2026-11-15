// Package events publishes facts from one govengers service to others on top of
// github.com/govengers/jobs: jobs stay inside a service, events cross services, and an event
// arrives as a job.
//
// A publisher declares a Topic in its contract package and publishes inside the transaction of
// the change (postgresadapter.Store.Publish). A local subscription (same database) gets its
// handler job enqueued in that transaction; a remote one gets a delivery in the ledger, which
// the Bus's outbound job POSTs as a CloudEvent, signed per Standard Webhooks, to the subscriber's
// inbox (Bus.InboxHandler). The inbox verifies the signature on the raw body, deduplicates and
// enqueues the handler job. Subscribers declare their subscriptions with Subscribe and send them
// to each publisher with Bus.Register (Bus.RegistrationHandler on the publisher's side).
// Delivery is at least once and unordered.
package events
