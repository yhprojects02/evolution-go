//go:build !cgo

package meowcaller

import "testing"

func TestOpusIsUnavailableWithoutLibopus(t *testing.T) {
	if audioCodecAvailable(AudioCodecOpus) {
		t.Fatal("Opus must not appear available without cgo and libopus")
	}
	if _, err := requireAudioCodec(AudioCodecOpus, false); err == nil {
		t.Fatal("one-to-one Opus must fail clearly without cgo and libopus")
	}
	codec, err := requireAudioCodec(AudioCodecOpus, true)
	if err != nil {
		t.Fatal(err)
	}
	if codec != AudioCodecMlow {
		t.Fatalf("group codec = %s, want MLow", codec)
	}
}
