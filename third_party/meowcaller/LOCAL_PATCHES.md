# Local meowcaller compatibility patch

This directory is based on `github.com/purpshell/meowcaller` at pinned upstream
revision `v0.0.0-20260726180203-6d9b7b2c1807` (commit
`6d9b7b2c1807`). The downloaded upstream module archive SHA-256 is
`c0a0bc41618314d36a99aa583849c60f55e67db415ed64288105bf1f256b6b25`.
`LICENSE` is retained. Modified upstream runtime files are `engine.go`,
`engine_media.go`, `group_media_receive.go`, `livecall.go`, and `video.go`.
These edits add codec selection dispatch, preserve direct-call MLow behavior
while enabling negotiated Opus for one-to-one calls, close native codec
resources, expose peer keyframe feedback, and optionally carry RTP timestamps
and orientation to compatible sinks. `engine_media.go` also creates a distinct
decoder for each MLow participant, preserving the upstream group receive
behavior. `go.mod` changes only the whatsmeow
replacement to this repository's retained fork. Added files are
`LOCAL_PATCHES.md`, `audio_codec.go`, `audio_codec_cgo.go`,
`audio_codec_nocgo.go`, `audio_codec_test.go`, and `audio_codec_nocgo_test.go`;
`engine_hook_test.go` verifies the pinned raw adapter can install against the
retained fork. The parent engine's browser media adapter separately handles
bounded Opus RTP reordering and timestamp-aligned PLC, then aggregates
variable-sized PCM callbacks into the 60 ms frames expected by meowcaller.
Upstream docs, examples, and datasheets are omitted; selected source-package
metadata files are retained in this copy.

`engine_media.go` dispatches the selected one-to-one audio codec through the
existing MLow implementation or a libopus encoder/decoder. Group media
continues to use MLow. Native Opus codecs implement idempotent `Close`; the
media loop and participant registry own their cleanup. `video.go` adds optional
timestamped and orientation sink interfaces; existing sinks remain
source-compatible. The local engine adapter uses them to preserve WhatsApp
RTP pacing and report rotation. Browser PLI can request a WhatsApp peer
keyframe through the existing encrypted SRTCP data channel. Keep this fork
pinned and compare changes with that upstream revision when updating it.
