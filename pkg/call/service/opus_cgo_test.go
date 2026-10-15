//go:build cgo

package call_service

import (
	"math"
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestOpusLibopusRoundTrip(t *testing.T) {
	encoder, err := newCallAudioEncoder()
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	decoder, err := newCallAudioDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	pcm := make([]float32, callAudioFrame*2)
	for i := range pcm {
		pcm[i] = 0.2
	}
	packet := make([]byte, 4000)
	n, err := encoder.Encode(pcm, packet)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("encoder returned an empty Opus packet")
	}
	decoded := make([]float32, 11520)
	decodedCount, err := decoder.Decode(packet[:n], decoded)
	if err != nil {
		t.Fatal(err)
	}
	if decodedCount != callAudioFrame*3 {
		t.Fatalf("decoded sample count = %d, want %d", decodedCount, callAudioFrame*3)
	}
	concealed := make([]float32, 11520)
	concealedCount, err := decoder.(interface {
		DecodePLC(int, []float32) (int, error)
	}).DecodePLC(decodedCount, concealed)
	if err != nil {
		t.Fatalf("decode Opus packet loss using PLC: %v", err)
	}
	if concealedCount != decodedCount {
		t.Fatalf("PLC sample count = %d, want timestamp-aligned frame size %d", concealedCount, decodedCount)
	}
	energy := float32(0)
	for _, sample := range concealed[:concealedCount*2] {
		energy += sample * sample
	}
	if energy == 0 {
		t.Fatal("Opus PLC returned silence after a non-silent decoded frame")
	}
}

func TestBrowserOpusIngressDecodesAndAggregates20msPackets(t *testing.T) {
	encoder, err := newOpusEncoder(48000)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	pcm := make([]float32, 960*2)
	for i := 0; i < 960; i++ {
		sample := float32(0.3 * math.Sin(2*math.Pi*440*float64(i)/48000))
		pcm[i*2], pcm[i*2+1] = sample, sample
	}
	encoded := make([]byte, 4000)
	n, err := encoder.Encode(pcm, encoded)
	if err != nil {
		t.Fatalf("encode 20 ms browser Opus packet: %v", err)
	}
	decoder, err := newCallAudioDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	ingress := &browserOpusIngress{decoder: decoder, pending: make([]float32, 0, callAudioFrame*2)}
	var frames [][]float32
	for i := uint16(0); i < 3; i++ {
		ready, processErr := ingress.Process(&rtp.Packet{
			Header:  rtp.Header{SequenceNumber: i + 1, Timestamp: uint32(i) * 960},
			Payload: append([]byte(nil), encoded[:int(n)]...),
		}, nil)
		if processErr != nil {
			t.Fatalf("decode browser packet %d: %v", i+1, processErr)
		}
		frames = append(frames, ready...)
	}
	if len(frames) != 1 || len(frames[0]) != callAudioFrame {
		t.Fatalf("three 20 ms packets produced %d frames with lengths %v, want one 960-sample/60 ms frame", len(frames), frameLengths(frames))
	}
	var energy float64
	for _, sample := range frames[0] {
		energy += float64(sample * sample)
	}
	if energy == 0 {
		t.Fatal("browser Opus ingress aggregated silence from a non-silent RTP payload")
	}
}

func TestWhatsAppAudioSinkAggregatesVariablePCMTo60msOpusSample(t *testing.T) {
	encoder, err := newCallAudioEncoder()
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	accumulator := &audioPCMAccumulator{}
	var frames [][]float32
	for _, size := range []int{320, 320, 320} { // three 20 ms peer frames
		input := make([]float32, size)
		for i := range input {
			input[i] = 0.2
		}
		ready := accumulator.Push(input)
		if len(ready) > 1 {
			t.Fatalf("one peer callback produced %d 60 ms frames", len(ready))
		}
		frames = append(frames, ready...)
	}
	if len(frames) != 1 || len(frames[0]) != callAudioFrame {
		t.Fatalf("peer PCM produced %d frames of lengths %v, want one 960-sample frame", len(frames), frameLengths(frames))
	}
	packet := make([]byte, 4000)
	n, err := encoder.Encode(monoToStereo(frames[0]), packet)
	if err != nil {
		t.Fatalf("encode 60 ms browser Opus sample: %v", err)
	}
	if n == 0 || callAudioSampleDuration != 60*time.Millisecond {
		t.Fatalf("RTP sample bytes=%d duration=%s, want encoded 60 ms sample", n, callAudioSampleDuration)
	}
}

func frameLengths(frames [][]float32) []int {
	lengths := make([]int, len(frames))
	for i := range frames {
		lengths[i] = len(frames[i])
	}
	return lengths
}
