//go:build !cgo

package call_service

import "errors"

type callAudioEncoder interface {
	Encode([]float32, []byte) (int, error)
	Close() error
}

type callAudioDecoder interface {
	Decode([]byte, []float32) (int, error)
	Close() error
}

var errOpusRequiresCGO = errors.New("browser call audio requires cgo and libopus")

func newCallAudioEncoder() (callAudioEncoder, error) { return nil, errOpusRequiresCGO }

func newCallAudioDecoder() (callAudioDecoder, error) { return nil, errOpusRequiresCGO }
