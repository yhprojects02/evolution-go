//go:build !cgo

package meowcaller

import "errors"

var errOpusRequiresCGO = errors.New("meowcaller: Opus requires cgo and libopus")

type unavailableAudioEncoder struct{}
type unavailableAudioDecoder struct{}

func opusCodecAvailable() bool                              { return false }
func newOpusFrameEncoder() (audioFrameEncoder, error)       { return nil, errOpusRequiresCGO }
func newOpusFrameDecoder() (participantAudioDecoder, error) { return nil, errOpusRequiresCGO }
func (*unavailableAudioDecoder) Decode([]byte) []float32    { return make([]float32, FrameSamples) }
