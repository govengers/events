# Security

## Signatures

Every delivery is signed per Standard Webhooks: `webhook-id` (the event id), `webhook-timestamp`, `webhook-signature: v1,<base64 HMAC-SHA256 of "id.timestamp.body">`. `events.Verify` accepts any of several signatures (secret rotation) and refuses a timestamp more than `SignatureTolerance` (5 minutes) away. Receivers outside govengers use any Standard Webhooks library with the subscription's `whsec_` secret.

## Secrets

- Service to service: the subscriber derives each subscription's secret as HMAC-SHA256(`InboxKey`, subscription name) (`DeriveSecret`), sends it at registration and stores nothing. Rotate by changing `InboxKey` and registering again; deliveries signed with the old secret in between are refused (401) and retried (401 and 403 retry like 404 and do not count towards the auto-disable while they retry, about 19 hours; one still refused at its last attempt counts), so they arrive once the registration reached the publisher. A registration job carries an ID of the inbox key (a hash, not the key): only a replica holding that key sends it, an older replica retries it.
- Webhooks an application creates for an external receiver: `events.NewSecret()`, shown to the user once, stored with `PutSubscriptions`.
- At rest the publisher seals every secret with `postgresadapter.Options.Sealer` (`events.Sealer`), bound to its row by the associated data `govengers_event_subscriptions/<subscriber>/<name>`. No sealer, no remote subscription (`ErrNoSealer`). govengers/auth's seal package has another method set (`Seal(seal.AssociatedData, []byte) (string, error)`); events does not import auth, so wrap the key ring:

```go
type authSealer struct{ ring *seal.Sealer }

func (s authSealer) data(associatedData []byte) seal.AssociatedData {
    return seal.AssociatedData{Purpose: "webhook_signing_secret", Table: "govengers_event_subscriptions", Row: string(associatedData)}
}

func (s authSealer) Seal(plaintext, associatedData []byte) ([]byte, error) {
    sealed, err := s.ring.Seal(s.data(associatedData), plaintext)
    return []byte(sealed), err
}

func (s authSealer) Open(sealed, associatedData []byte) ([]byte, error) {
    return s.ring.Open(s.data(associatedData), string(sealed))
}
```

  The sealed text is stored as `bytea`. Moving to a new primary key: `Reseal` every `sealed_secret` with the same wrapper, or let the subscribers register again (each registration seals anew).
- Registrations carry the API key and every signing secret: publisher URLs and `InboxURL` must be https unless `Options.AllowHTTP` (also needed for http endpoints in registrations). `Authorize` gets the registration's subscriptions with their endpoints, so an application can restrict where a subscriber's deliveries may go, which matters with `AllowPrivateNetworks`. A registration naming the publisher itself is refused.

## Outbound requests: safehttp

`safehttp.New` builds the client for URLs someone else chose. The address is checked in the dialer after DNS resolution, on every connection and redirect:

| Destination | Default | `AllowPrivateNetworks` |
|-------------|---------|------------------------|
| public | allowed | allowed |
| RFC 1918, fc00::/7, 100.64.0.0/10 | refused | allowed |
| loopback, link-local, metadata (169.254.169.254, fd00:ec2::254, 100.100.100.200, Azure WireServer 168.63.129.16), unspecified, multicast, NAT64, 6to4, site-local fec0::/10 | refused | refused |

`AllowedNetworks` narrows to a list and cannot lift a refusal. No proxy from the environment, `MaximumRedirects` (default 0; a delivery answered with a redirect past it fails at once), `MaximumResponseBytes` (default 1 MiB, an error past it), `Timeout` (default 30 s).

The Bus uses `safehttp.New(nil)` when `Options.HTTPClient` is nil, which refuses private networks. Inside a cluster pass `AllowPrivateNetworks: true`, better an `AllowedNetworks` with the service CIDR. When one Bus delivers both inside the cluster and to user-entered URLs, the client allows both; see the open item in [design.md](../design.md).

## Identities

The `actor` extension is audit information. A subscriber's handler runs as the subscriber's own system identity, never as the publisher's actor (E9). Local subscriptions carry the real identity through jobs' propagators, as any job does.

## Registration

The registration route must sit behind authentication; `Authorize` checks that the authenticated caller is the subscriber it registers and may read the topics. The inbox must not: its requests are authenticated by the signature.
