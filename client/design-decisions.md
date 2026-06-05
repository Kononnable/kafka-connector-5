# Design Decisions

### Rebootstrap always-on (no `metadata.recovery.strategy`)

KIP-899 (3.8.0) introduced `metadata.recovery.strategy` defaulting to
`none`. KIP-1102 (4.0.0) changed the default to `rebootstrap` and added
reliable trigger conditions (timeout + `REBOOTSTRAP_REQUIRED` error code).
The upstream Kafka client retains the `none` option for
backward-compatibility with pre-4.0 configs. This client omits it
entirely — rebootstrap is always active with just the timeout as a
tuneable, since there is no reason to ever disable it.

## Implemented KIPs

| KIP | Version | What |
|-----|---------|------|
| KIP-19 | 0.9.0.0 | Request timeout (`request_timeout`) |
| KIP-35 | 0.10.0.0 | ApiVersions request/response — version negotiation with v0 fallback |
| KIP-97 | 0.10.2.0 | RPC compatibility — `min(client_max, broker_max)` negotiation |
| KIP-117 | 0.11.0.0 | Periodic metadata refresh (`metadata.max.age.ms`) |
| KIP-144 | 0.11.0.0 | Exponential backoff for reconnect — `MIN(max, base * 2^(n-1)) * jitter(0.8..1.2)` |
| KIP-601 | 2.7.0 | Configurable socket connection timeout |
| KIP-899 | 3.8.0 | Rebootstrap when all known brokers are unavailable |
| KIP-1102 | 4.0.0 | Rebootstrap triggers: timeout + `REBOOTSTRAP_REQUIRED` error code |
