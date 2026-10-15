//go:build cgo

package meowcaller

import (
	"math"
	"testing"

	"github.com/rs/zerolog"
)

func TestNegotiatedAudioCodecDispatchHasRealPCM(t *testing.T) {
	log := zerolog.Nop()
	for _, codec := range []AudioCodec{AudioCodecMlow, AudioCodecOpus} {
		encoder, err := newCallAudioEncoder(codec, log)
		if err != nil {
			t.Fatalf("create %s encoder: %v", codec, err)
		}
		decoder, err := newCallAudioDecoder(codec, log)
		if err != nil {
			t.Fatalf("create %s decoder: %v", codec, err)
		}
		frame := make([]float32, FrameSamples)
		for i := range frame {
			frame[i] = float32(0.35 * math.Sin(2*math.Pi*440*float64(i)/SampleRate))
		}
		payload, err := encoder.Encode(frame)
		if err != nil {
			t.Fatalf("encode %s frame: %v", codec, err)
		}
		decoded := decoder.Decode(payload)
		if len(decoded) == 0 {
			t.Fatalf("%s decoder returned no PCM", codec)
		}
		var rms float64
		for _, sample := range decoded {
			rms += float64(sample * sample)
		}
		rms = math.Sqrt(rms / float64(len(decoded)))
		if rms < 0.001 {
			t.Fatalf("%s decoded RMS %.6f, want non-silent audio", codec, rms)
		}
		encoderCloser, encoderHasClose := encoder.(interface{ Close() error })
		decoderCloser, decoderHasClose := decoder.(interface{ Close() error })
		if codec == AudioCodecOpus && (!encoderHasClose || !decoderHasClose) {
			t.Fatal("Opus codecs must expose deterministic native resource cleanup")
		}
		if encoderHasClose {
			if err = encoderCloser.Close(); err != nil {
				t.Fatal(err)
			}
			if err = encoderCloser.Close(); err != nil {
				t.Fatalf("repeat encoder close: %v", err)
			}
		}
		if decoderHasClose {
			if err = decoderCloser.Close(); err != nil {
				t.Fatal(err)
			}
			if err = decoderCloser.Close(); err != nil {
				t.Fatalf("repeat decoder close: %v", err)
			}
		}
	}
}

func TestSelectedOneToOneOpusCodecUsesOpusEncoderAndDecoder(t *testing.T) {
	eng := &engine{calls: map[string]*engineCall{"selected": {codec: AudioCodecOpus}}}
	codec, group, ok := selectedCallCodec(eng, "selected")
	if !ok || group || codec != AudioCodecOpus {
		t.Fatalf("selected codec = %s group=%t found=%t", codec, group, ok)
	}
	enc, err := newCallAudioEncoder(codec, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	dec, err := newCallAudioDecoder(codec, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer enc.(interface{ Close() error }).Close()
	defer dec.(interface{ Close() error }).Close()
	input := make([]float32, FrameSamples)
	for i := range input {
		input[i] = float32(0.25 * math.Sin(2*math.Pi*440*float64(i)/SampleRate))
	}
	packet, err := enc.Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	output := dec.Decode(packet)
	var energy float64
	for _, sample := range output {
		energy += float64(sample * sample)
	}
	if len(output) == 0 || energy == 0 {
		t.Fatal("selected Opus dispatch returned silent or empty PCM")
	}
}

func TestGroupCallsRetainMLowAudioCodec(t *testing.T) {
	codec, err := requireAudioCodec(AudioCodecOpus, true)
	if err != nil {
		t.Fatal(err)
	}
	if codec != AudioCodecMlow {
		t.Fatalf("group codec = %s, want MLow", codec)
	}
}

func TestParticipantAudioDecoderFactoryKeepsMLowInstancesIndependent(t *testing.T) {
	log := zerolog.Nop()
	first, err := newCallAudioDecoder(AudioCodecMlow, log)
	if err != nil {
		t.Fatal(err)
	}
	factory := newParticipantAudioDecoderFactory(AudioCodecMlow, log, first)
	firstParticipant := factory()
	secondParticipant := factory()
	thirdParticipant := factory()
	if firstParticipant != first || firstParticipant == secondParticipant || secondParticipant == thirdParticipant {
		t.Fatal("MLow participant registry reused a decoder instance")
	}
}
