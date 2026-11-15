# Security

## Signatures

Every delivery is signed per Standard Webhooks: `webhook-id` (the event id), `webhook-timestamp`, `webhook-signature: v1,<base64 HMAC-SHA256 of "id.timestamp.body">`. `events.Verify` accepts any of several signatures (secret rotation) and refuses a timestamp more than `SignatureTolerance` (5 minutes) away. Receivers outside govengers use any Standard Webhooks library with the subscription's `whsec_` secret.

## Secrets

- Service to service: the subscriber derives each subscription's secret as HMAC-SHA256(`InboxKey`, subscription name) (`DeriveSecret`), sends it at registration and stores nothing. Rotate by changing `InboxKey` and registering again; deliveries signed with the old secret in between are refused (401) and retried.
- Webhooks an application creates for an external receiver: `events.NewSecret()`, shown to the user once, stored with `PutSubscriptions`.
- At rest the publisher seals every secret with `postgresadapter.Options.Sealer` (`events.Sealer`, the method set of govengers/auth's seal package), bound to its row by the associated data. No sealer, no remote subscription (`ErrNoSealer`).

## Outbound requests: safehttp

`safehttp.New` builds the client for URLs someone else chose. The address is checked in the dialer after DNS resolution, on every connection and redirect:

| Destination | Default | `AllowPrivateNetworks` |
|-------------|---------|------------------------|
| public | allowed | allowed |
| RFC 1918, fc00::/7, 100.64.0.0/10 | refused | allowed |
| loopback, link-local, metadata (169.254.169.254, fd00:ec2::254, 100.100.100.200), unspecified, multicast, NAT64, 6to4 | refused | refused |

`AllowedNetworks` narrows to a list and cannot lift a refusal. No proxy from the environment, `MaximumRedirects` (default 0), `MaximumResponseBytes` (default 1 MiB, an error past it), `Timeout` (default 30 s).

The Bus uses `safehttp.New(nil)` when `Options.HTTPClient` is nil, which refuses private networks. Inside a cluster pass `AllowPrivateNetworks: true`, better an `AllowedNetworks` with the service CIDR. When one Bus delivers both inside the cluster and to user-entered URLs, the client allows both; see the open item in [design.md](../design.md).

## Identities

The `actor` extension is audit information. A subscriber's handler runs as the subscriber's own system identity, never as the publisher's actor (E9). Local subscriptions carry the real identity through jobs' propagators, as any job does.

## Registration

The registration route must sit behind authentication; `Authorize` checks that the authenticated caller is the subscriber it registers and may read the topics. The inbox must not: its requests are authenticated by the signature.
