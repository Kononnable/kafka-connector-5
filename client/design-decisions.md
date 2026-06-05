# Design Decisions

## Implemented KIPs

| KIP | Version | What |
|-----|---------|------|
| KIP-19 | 0.9.0.0 | Request timeout (`request_timeout`) |
| KIP-35 | 0.10.0.0 | ApiVersions request/response — version negotiation with v0 fallback |
| KIP-97 | 0.10.2.0 | RPC compatibility — `min(client_max, broker_max)` negotiation |
| KIP-117 | 0.11.0.0 | Periodic metadata refresh (`metadata.max.age.ms`) |
| KIP-144 | 0.11.0.0 | Exponential backoff for reconnect — `MIN(max, base * 2^(n-1)) * jitter(0.8..1.2)` |
| KIP-601 | 2.7.0 | Configurable socket connection timeout |
