package call_service

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/purpshell/meowcaller"
)

func TestBrowserPCMQueueDiagnosticsTrackDropsAndDequeueAge(t *testing.T) {
	call := &liveCall{
		audioIn:          make(chan queuedAudioFrame, 1),
		mediaDiagnostics: newCallMediaDiagnostics(),
	}
	call.mediaDiagnostics.browserPCMQueue.capacity = 1
	first := make([]float32, callAudioFrame)
	second := make([]float32, callAudioFrame)
	second[0] = 0.5
	queueAudioFrames(call, [][]float32{first, second})
	time.Sleep(2 * time.Millisecond)

	got, err := (&browserAudioSource{frames: call.audioIn, stats: &call.mediaDiagnostics.browserPCMQueue}).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != second[0] {
		t.Fatalf("drop-oldest queue returned stale frame: first sample %v", got[0])
	}
	stats := call.mediaDiagnostics.browserPCMQueue.snapshot()
	if stats.Depth != 0 || stats.MaxDepth != 1 || stats.OverflowDrops != 1 || stats.MaxDequeuedAgeMs < 1 {
		t.Fatalf("unexpected queue diagnostics: %+v", stats)
	}
}

func TestCallMediaDiagnosticsJSONContainsOnlySanitizedMeasurements(t *testing.T) {
	srtt := 22.0
	relay := meowcaller.MediaTransportDiagnostics{Available: true, SRTTMs: &srtt, BufferedAmountBytes: 1024, BytesSent: 4096, BytesReceived: 2048}
	diagnostics := CallMediaDiagnostics{
		BrowserRTPQueue: QueueDiagnostics{Depth: 1, Capacity: 32, MaxDepth: 2, MaxDequeuedAgeMs: 11, OverflowDrops: 1},
		WhatsAppRelay:   &relay,
	}
	encoded, err := json.Marshal(diagnostics)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, forbidden := range []string{"controllerId", "callId", "peer", "sdp", "candidateId", "address", "token", "secret-peer"} {
		if strings.Contains(strings.ToLower(text), strings.ToLower(forbidden)) {
			t.Fatalf("diagnostics serialized forbidden field %q: %s", forbidden, text)
		}
	}
	if strings.Contains(text, "secret") {
		t.Fatalf("diagnostics serialized sensitive value: %s", text)
	}
}

func TestMediaTransportSamplerCanBeClearedWhileCurrentIsRead(t *testing.T) {
	call := &meowcaller.Call{}
	sampler := func() (meowcaller.MediaTransportDiagnostics, bool) {
		srtt := 12.0
		return meowcaller.MediaTransportDiagnostics{Available: true, SRTTMs: &srtt}, true
	}
	call.SetMediaTransportDiagnostics(sampler)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 1000; j++ {
				call.CurrentMediaTransportDiagnostics()
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 1000; i++ {
			call.SetMediaTransportDiagnostics(nil)
			call.SetMediaTransportDiagnostics(sampler)
		}
	}()
	workers.Wait()
}

func TestCurrentMediaDiagnosticsCannotCrossCallReplacement(t *testing.T) {
	active := &liveCall{
		envelope:         CallEnvelope{CallID: "replacement", InstanceID: "instance", State: CallStateActive},
		mediaDiagnostics: newCallMediaDiagnostics(),
	}
	runtime := &Runtime{
		instances: map[string]*instanceCalls{"instance": {active: "replacement"}},
		calls:     map[string]*liveCall{"replacement": active},
	}
	if _, ok := runtime.CurrentMediaDiagnostics("instance", "previous"); ok {
		t.Fatal("returned replacement-call diagnostics for an earlier call snapshot")
	}
	if _, ok := runtime.CurrentMediaDiagnostics("other-instance", "replacement"); ok {
		t.Fatal("returned diagnostics for a different instance")
	}
}

func TestHeartbeatStoresSanitizedBrowserStatsOnlyForOwnerAndClearsOnClose(t *testing.T) {
	call := &liveCall{owner: "owner", envelope: CallEnvelope{CallID: "call", InstanceID: "instance", State: CallStateActive}}
	runtime := &Runtime{calls: map[string]*liveCall{"call": call}}
	badOwnerStats := &BrowserMediaStats{Transport: &BrowserTransportStats{RelayProtocol: "tls"}}
	if err := runtime.Heartbeat("instance", "call", "other-owner", badOwnerStats); !errors.Is(err, ErrOwner) {
		t.Fatalf("wrong owner heartbeat error = %v", err)
	}
	call.mu.Lock()
	if call.browserMediaStats != nil {
		call.mu.Unlock()
		t.Fatal("wrong owner changed volatile browser stats")
	}
	call.mu.Unlock()

	rtt := 20.0
	invalid := math.NaN()
	received := uint64(21)
	lost := int64(-1)
	input := &BrowserMediaStats{
		Transport:    &BrowserTransportStats{LocalCandidateType: "relay", LocalProtocol: "udp", RelayProtocol: "secret", CurrentRoundTripMs: &rtt},
		InboundAudio: &BrowserInboundAudioStats{AverageJitterBufferMs: &invalid, PacketsLost: &lost, PacketsReceived: &received},
	}
	if err := runtime.Heartbeat("instance", "call", "owner", input); err != nil {
		t.Fatal(err)
	}
	call.mu.Lock()
	stored := call.browserMediaStats
	call.mu.Unlock()
	if stored == nil || stored.Transport == nil || stored.Transport.RelayProtocol != "" || stored.Transport.CurrentRoundTripMs == nil || *stored.Transport.CurrentRoundTripMs != 20 || stored.InboundAudio == nil || stored.InboundAudio.AverageJitterBufferMs != nil || stored.InboundAudio.PacketsLost == nil || *stored.InboundAudio.PacketsLost != -1 {
		t.Fatalf("browser stats were not selectively sanitized: %+v", stored)
	}
	call.closeMedia()
	call.mu.Lock()
	defer call.mu.Unlock()
	if call.browserMediaStats != nil {
		t.Fatal("browser stats survived call media teardown")
	}
}

func TestMediaQueueProducerConsumerAndSnapshotAreRaceSafe(t *testing.T) {
	call := &liveCall{audioIn: make(chan queuedAudioFrame, 12), mediaDiagnostics: newCallMediaDiagnostics()}
	source := &browserAudioSource{frames: call.audioIn, stats: &call.mediaDiagnostics.browserPCMQueue}
	done := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		frame := make([]float32, callAudioFrame)
		for i := 0; i < 1000; i++ {
			queueAudioFrames(call, [][]float32{frame})
		}
		close(done)
	}()
	go func() {
		defer workers.Done()
		for {
			select {
			case <-done:
				for len(call.audioIn) > 0 {
					_, _ = source.ReadFrame()
				}
				return
			default:
				_, _ = source.ReadFrame()
			}
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 1000; i++ {
			call.mediaDiagnostics.browserPCMQueue.snapshot()
		}
	}()
	workers.Wait()
}
