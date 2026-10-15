package meowcaller

import (
	"fmt"
	"sync"

	"github.com/purpshell/meowcaller/mlow"
	"github.com/rs/zerolog"
)

type audioFrameEncoder interface {
	Encode([]float32) ([]byte, error)
}

type unavailableParticipantAudioDecoder struct{}

func (*unavailableParticipantAudioDecoder) Decode([]byte) []float32 {
	return make([]float32, FrameSamples)
}

func newParticipantAudioDecoderFactory(codec AudioCodec, log zerolog.Logger, first participantAudioDecoder) func() participantAudioDecoder {
	var mu sync.Mutex
	return func() participantAudioDecoder {
		mu.Lock()
		defer mu.Unlock()
		if first != nil {
			decoder := first
			first = nil
			return decoder
		}
		if codec == AudioCodecMlow {
			return mlow.NewMlowDecoder(mlow.WithLogger(log))
		}
		decoder, err := newCallAudioDecoder(codec, log)
		if err != nil {
			log.Error().Err(err).Msg("create participant audio decoder")
			return &unavailableParticipantAudioDecoder{}
		}
		return decoder
	}
}

type mlowFrameEncoder struct{ encoder *mlow.MlowEncoder }

func (e *mlowFrameEncoder) Encode(frame []float32) ([]byte, error) { return e.encoder.Encode(frame) }

func newCallAudioEncoder(codec AudioCodec, log zerolog.Logger) (audioFrameEncoder, error) {
	if codec == AudioCodecOpus {
		return newOpusFrameEncoder()
	}
	return &mlowFrameEncoder{encoder: mlow.NewMlowEncoder(mlow.WithLogger(log))}, nil
}

func newCallAudioDecoder(codec AudioCodec, log zerolog.Logger) (participantAudioDecoder, error) {
	if codec == AudioCodecOpus {
		return newOpusFrameDecoder()
	}
	return mlow.NewMlowDecoder(mlow.WithLogger(log)), nil
}

func audioCodecAvailable(codec AudioCodec) bool {
	if codec != AudioCodecOpus {
		return true
	}
	return opusCodecAvailable()
}

func selectedCallCodec(e *engine, callID string) (AudioCodec, bool, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[callID]
	if m == nil {
		return AudioCodecMlow, false, false
	}
	return m.codec, m.group, true
}

func requireAudioCodec(codec AudioCodec, group bool) (AudioCodec, error) {
	if group {
		return AudioCodecMlow, nil
	}
	if !audioCodecAvailable(codec) {
		return codec, fmt.Errorf("meowcaller: negotiated %s audio requires cgo and libopus", codec)
	}
	return codec, nil
}
