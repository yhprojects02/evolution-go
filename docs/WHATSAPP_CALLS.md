# Browser to WhatsApp calls (engine contract)

The engine owns the active call state and bridges browser WebRTC media through Pion to the paired WhatsApp client through the pinned `meowcaller` adapter. This feature is disabled unless `CALLS_ENABLED=true`. Registration installs the adapter immediately after `whatsmeow.NewClient` and before `Connect`; reconnects reuse the same whatsmeow client and companion device.

## Release configuration

| Variable | Meaning |
|---|---|
| `CALLS_ENABLED` | Master feature switch; default `false`. |
| `CALLS_ALLOWLIST` | Comma-separated phone numbers. Both the logged-in device JID and call peer must match. `*` enables all Evolution peers after explicit release approval. |
| `CALLS_MAX_CONCURRENT` | Process-wide concurrent call limit, capped at four. |
| `CALLS_MEDIA_UDP_BIND` | Pion UDP mux bind address; default `0.0.0.0`. |
| `CALLS_MEDIA_UDP_PORT` | Pion UDP mux port; default `40000`. |
| `CALLS_PUBLIC_IP` | Public address advertised in host ICE candidates. |
| `TURN_HOST` | TURN host, without a scheme. The engine returns UDP/TCP 3478 and TLS 443 URLs. |
| `TURN_SHARED_SECRET` | Coturn REST shared secret; never sent to browsers. |
| `CALLS_TURN_TTL_SECONDS` | TURN credential lifetime; default ten minutes. |

The Docker builder cross-compiles static libopus 1.5.2 for the selected Linux target with Zig, alongside the existing WebP cgo dependencies. The runtime image does not need a separate libopus package.

## Authenticated HTTP API

All routes below use the existing instance `apikey` authentication. JSON keys are camelCase.

The deployment-only `POST /call/deploy-drain` route uses the existing global `AuthAdmin` key and is not proxied through Middleware or Demo. Send `{ "owner": "<random 128-bit-or-longer nonce>", "action": "acquire"|"release" }`. Acquire atomically gates new calls and returns top-level `{ "owner": "...", "acquired": true, "active": 0, "pending": 0 }`; `active` counts ringing/connecting/active sessions and `pending` counts outbound reservations. It holds the gate even when either count is nonzero, so deployment automation must release immediately when refusing. Existing media is not changed. Same-owner acquire is idempotent; a competing owner receives 409. Release by the held owner returns `{ "acquired": false, "active": 0, "pending": 0 }`; a repeated release while already open is a no-op success. Invalid owners/actions receive 400. This endpoint exposes no peer or instance identifiers.

| Route | Request | Response |
|---|---|---|
| `POST /call/dial` | `{ "peer": "601162528788", "media": "audio"|"video", "idempotencyKey": "<random 128-bit-or-longer value>" }` | Call envelope, including the initial controller capability. |
| `POST /call/answer` | `{ "callId": "...", "requestId": "<random 128-bit-or-longer value>" }` | Call envelope and controller capability for the first successful answer claimant. |
| `POST /call/reject` | `{ "callId": "..." }` declines an unclaimed inbound ring. `{ "callId": "...", "controllerId": "..." }` rejects an owned call. Legacy `{ "callCreator": "...", "callId": "..." }` remains supported while the feature is off. | Ended envelope for managed calls; legacy response remains `{ "message": "success" }`. |
| `POST /call/hangup` | `{ "callId": "...", "controllerId": "..." }` | Ended envelope. |
| `GET /call/current` | — | `{ "call": <redacted envelope> }`, or `{ "call": null }`. |
| `POST /call/signal/offer` | `{ "callId": "...", "controllerId": "...", "sdp": "..." }` | `{ "callId": "...", "type": "answer", "sdp": "..." }` after server ICE gathering. |
| `POST /call/signal/candidate` | `{ "callId": "...", "controllerId": "...", "candidate": "...", "sdpMid": "...", "sdpMLineIndex": 0, "usernameFragment": "..." }` | `{ "message": "success" }`. `sdpMid`, `sdpMLineIndex`, and `usernameFragment` are optional. |
| `POST /call/heartbeat` | `{ "callId": "...", "controllerId": "..." }` | `{ "message": "success" }`. |
| `GET /call/ice-config` | — | `{ "iceServers": [...], "expiresAt": "..." }`, with short-lived TURN credentials when configured. |

Controller IDs are 256-bit random bearer capabilities. They appear only in the initial dial/answer response; current-state polling, idempotency replays, and `CALL` webhook events always redact them. `requestId` and `idempotencyKey` must be treated as random capabilities and kept private. An idempotency replay returns state without a controller token, so a client that loses the original response cannot recover ownership from polling; a missed dial response is bounded by the controller heartbeat timeout. The browser should persist the controller token in `sessionStorage` so a page refresh by the same tab can continue the call.

Call envelopes contain `callId`, `instanceId`, `peer`, `direction`, `media`, `state`, monotonically increasing `version`, `createdAt`, `updatedAt`, optional `connectedAt`, `endedAt`, `endReason`, and `videoOrientation`. All timestamps are Unix epoch milliseconds. `connectedAt` is set once when the call first becomes active and is stable across browser reconnection; `endedAt` is set once at terminal transition. `videoOrientation`, when present, is clockwise quarter-turns (0–3) from WhatsApp video metadata. `controllerId` is present only in the initial dial/answer response. The state set is `dialing`, `ringing`, `connecting`, `active`, `ended`. `active` requires both WhatsApp acceptance/media readiness and a connected browser peer. End reasons include `rejected`, `reject`, `hangup`, `whatsapp_disconnected`, `whatsapp_connect_failure`, `instance_disconnected`, `instance_logout`, `instance_deleted`, `engine_shutdown`, `controller_timeout`, `dial_timeout`, `ring_timeout`, and browser-media timeout/disconnect reasons.

Managed state changes emit `{ "event": "CallState", "instance": "<instanceId>", "data": <redacted envelope> }` through the existing `CALL` subscription/webhook/queue path. `GET /call/current` returns `{ "call": <redacted envelope> }` or `{ "call": null }` for recovery after missed events. The engine does not expose call history; the Worker/D1 layer owns durable history. The engine never auto-answers inbound calls. An authenticated instance can decline an inbound ring before browser claim; only the first answer claimant can control signaling or media. A claimed owner can submit a fresh SDP offer after page refresh, replacing its stale Pion peer while retaining the call and controller token.

## Media and bounded behavior

Browser media terminates at Pion on the VPS. Opus RTP uses a 48 kHz clock. Browser Opus is decoded at 48 kHz, downmixed/resampled to mono 16 kHz PCM, aggregated into meowcaller 60 ms frames, and queued with a twelve-frame bound that drops the oldest frame on overflow. The return path encodes WhatsApp mono 16 kHz PCM as browser-compatible stereo Opus RTP. H.264 RTP is reordered in a bounded sample builder, preserving frame boundaries and 90 kHz timestamps while depacketizing to Annex-B access units for meowcaller; WhatsApp Annex-B access units are packetized by Pion with frame durations derived from source RTP timestamps (the first frame uses a 24 fps fallback). PLI feedback is drained from both Pion senders: WhatsApp keyframe requests reach the browser, and browser requests are encrypted and forwarded to the WhatsApp peer. WhatsApp video orientation is published in the call envelope. Offer signaling returns a fully gathered SDP answer; trickled browser candidates can also be submitted after the offer using the candidate route. ICE server responses contain `iceServers[].urls`, and when TURN is configured also `username`, `credential`, and RFC3339 `expiresAt`.

There is one call per instance and per canonical WhatsApp account phone, plus at most four across the process. The allowlist is evaluated against both the local device's resolved phone and the peer's resolved phone; group and broadcast targets are rejected, and LIDs require a local phone mapping. Unanswered inbound rings end after sixty seconds; dialing and browser setup have bounded timeouts; browser loss has a fifteen-second grace; a claimed controller without heartbeat is cleaned up after forty-five seconds. Instance logout, disconnect, deletion, and process shutdown terminate active calls and close Pion resources. No recording, transcription, group call, screen sharing, or audio-to-video upgrade is implemented.

Build or loopback test success proves codec and peer setup only. It does not prove end-to-end WhatsApp acceptance, live account compatibility, NAT reachability, subjective audio quality, or production behavior.
