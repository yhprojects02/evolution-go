package call_service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func TestMeowcallerRawCallAdapterInstallsBeforeConnect(t *testing.T) {
	device := &store.Device{ID: ptrJID(types.NewJID("60183568788", types.DefaultUserServer))}
	wa := whatsmeow.NewClient(device, nil)
	if wa.IsConnected() {
		t.Fatal("fresh whatsmeow client unexpectedly connected")
	}

	field := reflect.ValueOf(wa).Elem().FieldByName("nodeHandlers")
	if !field.IsValid() || field.Kind() != reflect.Map {
		t.Fatal("local whatsmeow nodeHandlers layout changed")
	}
	beforeCall := field.MapIndex(reflect.ValueOf("call")).Pointer()
	_ = meowcaller.NewClient(wa)
	afterCall := field.MapIndex(reflect.ValueOf("call")).Pointer()
	if afterCall == 0 || afterCall == beforeCall {
		t.Fatal("meowcaller did not wrap the raw call node handler")
	}
	if field.MapIndex(reflect.ValueOf("ack")).IsNil() {
		t.Fatal("meowcaller did not install the raw ack node handler")
	}
}

func TestDisabledRuntimeDoesNotInstallCallHooks(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{}, nil)
	field := reflect.ValueOf(wa).Elem().FieldByName("nodeHandlers")
	beforeCall := field.MapIndex(reflect.ValueOf("call")).Pointer()
	runtime := NewRuntime(RuntimeOptions{Enabled: false})
	if err := runtime.RegisterInstance(&instance_model.Instance{Id: "disabled"}, wa); err != nil {
		t.Fatal(err)
	}
	afterCall := field.MapIndex(reflect.ValueOf("call")).Pointer()
	if beforeCall != afterCall {
		t.Fatal("disabled runtime replaced WhatsApp's existing call handler")
	}
	if runtime.instances["disabled"].caller != nil {
		t.Fatal("disabled runtime initialized meowcaller")
	}
	runtime.addIncoming("missing", nil) // feature-off callback is a no-op, with no auto-reject path
	_ = runtime.Close()
}

func TestAllowlistUsesLocalCanonicalPhoneAndPeer(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{ID: ptrJID(types.NewJID("60183568788", types.DefaultUserServer))}, nil)
	runtime := NewRuntime(RuntimeOptions{Enabled: true, Allowlist: []string{"+60 18-356-8788", "601162528788"}})
	instance := &instance_model.Instance{Id: "anon-account"}
	if err := runtime.RegisterInstance(instance, wa); err != nil {
		t.Fatal(err)
	}
	if !runtime.EnabledFor(instance.Id) {
		t.Fatal("allowlisted local account was rejected")
	}
	if !runtime.peerAllowed("601162528788@s.whatsapp.net") {
		t.Fatal("allowlisted peer was rejected")
	}
	if runtime.peerAllowed("601199999999@s.whatsapp.net") {
		t.Fatal("non-allowlisted peer was admitted")
	}
	if runtime.EnabledFor("60183568788") {
		t.Fatal("allowlisted phone was incorrectly treated as an instance ID")
	}
	_ = runtime.Close()
}

func TestAccountPhoneRefreshAfterPairing(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{}, nil)
	runtime := NewRuntime(RuntimeOptions{Enabled: true, Allowlist: []string{"60183568788"}})
	instance := &instance_model.Instance{Id: "qr-alias"}
	if err := runtime.RegisterInstance(instance, wa); err != nil {
		t.Fatal(err)
	}
	if runtime.EnabledFor(instance.Id) {
		t.Fatal("unpaired account was allowed")
	}
	wa.Store.ID = ptrJID(types.NewJID("60183568788", types.DefaultUserServer))
	if !runtime.RefreshInstancePhone(instance.Id) {
		t.Fatal("paired account phone was not refreshed")
	}
	if !runtime.EnabledFor(instance.Id) {
		t.Fatal("paired account remained disabled after identity refresh")
	}
	_ = runtime.Close()
}

func TestAccountReservationsBlockAliasesAndRejectGroups(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{Enabled: true, Allowlist: []string{"60183568788"}, MaxActive: 4})
	for _, id := range []string{"qr-alias", "phone-route"} {
		wa := whatsmeow.NewClient(&store.Device{ID: ptrJID(types.NewJID("60183568788", types.DefaultUserServer))}, nil)
		if err := runtime.RegisterInstance(&instance_model.Instance{Id: id}, wa); err != nil {
			t.Fatal(err)
		}
	}
	runtime.mu.Lock()
	if err := runtime.reservePhoneLocked(runtime.instances["qr-alias"], "qr-alias"); err != nil {
		t.Fatalf("reserve first alias: %v", err)
	}
	if err := runtime.reservePhoneLocked(runtime.instances["phone-route"], "phone-route"); err != ErrCallBusy {
		t.Fatalf("second alias reservation = %v, want busy", err)
	}
	runtime.mu.Unlock()
	if _, _, err := runtime.resolvePeer("qr-alias", "123456789@g.us"); err == nil {
		t.Fatal("group JID was accepted as a one-to-one peer")
	}
	_ = runtime.Close()
}

func TestCallRuntimeRequiresExplicitAllowlist(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{Enabled: true})
	if runtime.EnabledFor("any-instance") {
		t.Fatal("empty allowlist enabled browser calls")
	}
}

func TestDeploymentDrainCountsAndGatesPendingWithoutEndingCalls(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{Enabled: true, Allowlist: []string{"*"}})
	entry := &instanceCalls{instance: &instance_model.Instance{Id: "instance"}, accountPhone: "60183568788", active: "pending"}
	call := &liveCall{envelope: CallEnvelope{CallID: "live", InstanceID: "other", State: CallStateActive, Version: 3, ConnectedAt: 10}}
	runtime.mu.Lock()
	runtime.instances["instance"] = entry
	runtime.calls["live"] = call
	runtime.active = 2
	runtime.phoneCalls[entry.accountPhone] = "pending:instance"
	runtime.mu.Unlock()
	const owner = "release-nonce-0123456789abcdef0123456789abcdef"
	status, err := runtime.DeploymentDrain(owner, "acquire")
	if err != nil || !status.Acquired || status.Active != 1 || status.Pending != 1 || status.Owner != owner {
		t.Fatalf("acquire status = %+v, %v", status, err)
	}
	runtime.mu.Lock()
	err = runtime.reservePhoneLocked(entry, "instance")
	runtime.mu.Unlock()
	if err != ErrCallDrainConflict {
		t.Fatalf("new reservation during drain = %v", err)
	}
	if _, err = runtime.DeploymentDrain("different-release-0123456789abcdef0123456789abcdef", "acquire"); err != ErrCallDrainConflict {
		t.Fatalf("competing acquire = %v", err)
	}
	if call.envelope.State != CallStateActive || call.envelope.ConnectedAt != 10 {
		t.Fatalf("drain altered existing call: %+v", call.envelope)
	}
	if _, err = runtime.DeploymentDrain(owner, "release"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err = runtime.DeploymentDrain(owner, "release"); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if runtime.drainOwner != "" {
		t.Fatal("release left the runtime drained")
	}
}

func TestFirstAnswerClaimIsAtomicAndSnapshotsHideController(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{Enabled: true, Allowlist: []string{"*"}})
	instance := &instance_model.Instance{Id: "instance"}
	callID := "call"
	runtime.mu.Lock()
	runtime.instances[instance.Id] = &instanceCalls{instance: instance}
	runtime.calls[callID] = &liveCall{envelope: CallEnvelope{CallID: callID, InstanceID: instance.Id, Direction: "inbound", State: CallStateRinging, Version: 1}}
	runtime.instances[instance.Id].active = callID
	runtime.active = 1
	runtime.mu.Unlock()

	ids := []string{fmt.Sprintf("%032x", 1), fmt.Sprintf("%032x", 2)}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, requestID := range ids {
		wg.Add(1)
		go func(requestID string) {
			defer wg.Done()
			_, _, _, err := runtime.claim(instance.Id, callID, requestID)
			results <- err
		}(requestID)
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if err != ErrCallConflict {
			t.Fatalf("claim error = %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("first-answer winners = %d, want 1", winners)
	}

	runtime.mu.Lock()
	winningID := runtime.calls[callID].claimID
	owner := runtime.calls[callID].owner
	runtime.mu.Unlock()
	if len(winningID) < 32 || len(owner) < 64 {
		t.Fatalf("winning request ID length=%d controller length=%d", len(winningID), len(owner))
	}
	snapshot, ok := runtime.Snapshot(instance.Id)
	if !ok {
		t.Fatal("active call snapshot missing")
	}
	if snapshot.ControllerID != "" {
		t.Fatal("redacted snapshot exposed controller token")
	}
}

func TestAnswerSameRequestIDIsSingleFlightAndIdempotent(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{})
	instanceID, callID := "instance", "inbound"
	var answerCalls atomic.Int32
	entered, continueAnswer := make(chan struct{}), make(chan struct{})
	var enteredOnce sync.Once
	live := &liveCall{envelope: CallEnvelope{CallID: callID, InstanceID: instanceID, Direction: "inbound", State: CallStateRinging, Version: 1}}
	live.answerFn = func() error {
		answerCalls.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-continueAnswer
		return nil
	}
	runtime.mu.Lock()
	runtime.instances[instanceID] = &instanceCalls{instance: &instance_model.Instance{Id: instanceID}}
	runtime.calls[callID] = live
	runtime.instances[instanceID].active = callID
	runtime.active = 1
	runtime.mu.Unlock()

	const count = 8
	start := make(chan struct{})
	results := make(chan CallEnvelope, count)
	errorsOut := make(chan error, count)
	var ready sync.WaitGroup
	for range count {
		ready.Add(1)
		go func() {
			ready.Done()
			<-start
			env, err := runtime.Answer(instanceID, callID, "request-0123456789abcdef0123456789abcdef")
			results <- env
			errorsOut <- err
		}()
	}
	ready.Wait()
	close(start)
	<-entered
	close(continueAnswer)

	var controllerResponses int
	for range count {
		env := <-results
		if env.ControllerID != "" {
			controllerResponses++
		}
		if err := <-errorsOut; err != nil {
			t.Fatal(err)
		}
	}
	if got := answerCalls.Load(); got != 1 {
		t.Fatalf("underlying Answer calls = %d, want 1", got)
	}
	if controllerResponses != 1 {
		t.Fatalf("responses containing controller capability = %d, want 1", controllerResponses)
	}

	live.mu.Lock()
	live.waReady, live.peerReady = true, true
	runtime.activateLocked(live)
	connectedAt := live.envelope.ConnectedAt
	activeVersion := live.envelope.Version
	live.mu.Unlock()
	if connectedAt == 0 {
		t.Fatal("connectedAt was not set on first active transition")
	}
	replay, err := runtime.Answer(instanceID, callID, "request-0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if replay.State != CallStateActive || replay.ControllerID != "" || replay.ConnectedAt != connectedAt || replay.Version != activeVersion {
		t.Fatalf("same-request replay changed active state or leaked owner: %+v", replay)
	}
	live.mu.Lock()
	runtime.endLocked(live, "rejected")
	live.mu.Unlock()
	replay, err = runtime.Answer(instanceID, callID, "request-0123456789abcdef0123456789abcdef")
	if err != nil || replay.State != CallStateEnded || replay.ControllerID != "" {
		t.Fatalf("same-request terminal replay = %+v, %v", replay, err)
	}
	_ = runtime.Close()
}

func TestLateWhatsAppPhasesCannotRegressAndTerminalTimesAreStable(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{})
	call := &liveCall{envelope: CallEnvelope{State: CallStateConnecting, Version: 4}}
	call.waReady, call.peerReady = true, true
	runtime.activateLocked(call)
	connectedAt, version := call.envelope.ConnectedAt, call.envelope.Version
	runtime.activateLocked(call)
	runtime.transitionLocked(call, CallStateRinging, "")
	runtime.transitionLocked(call, CallStateDialing, "")
	if call.envelope.State != CallStateActive || call.envelope.Version != version || connectedAt == 0 || call.envelope.ConnectedAt != connectedAt {
		t.Fatalf("duplicate readiness or stale phase regressed active state: %+v", call.envelope)
	}
	runtime.endLocked(call, "reject")
	endedAt, terminalVersion := call.envelope.EndedAt, call.envelope.Version
	runtime.endLocked(call, "whatsapp_ended")
	if call.envelope.State != CallStateEnded || call.envelope.EndedAt != endedAt || call.envelope.EndReason != "reject" || call.envelope.Version != terminalVersion {
		t.Fatalf("terminal timestamp/reason changed after authoritative end: %+v", call.envelope)
	}
}

func TestRepeatedOwnedControlReturnsTerminalState(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{})
	const owner = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	call := &liveCall{owner: owner, envelope: CallEnvelope{CallID: "ended", InstanceID: "instance", State: CallStateEnded, EndReason: "hangup", EndedAt: 123, UpdatedAt: 123}}
	runtime.mu.Lock()
	runtime.calls[call.envelope.CallID] = call
	runtime.mu.Unlock()
	for _, action := range []string{"hangup", "reject"} {
		env, err := runtime.Control("instance", "ended", owner, action)
		if err != nil || env.State != CallStateEnded || env.EndedAt != 123 {
			t.Fatalf("repeat %s returned %+v, %v", action, env, err)
		}
	}
}

func TestRejectUnclaimedWinsBeforeAnswerClaim(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{})
	instanceID, callID := "instance", "ringing"
	runtime.mu.Lock()
	runtime.instances[instanceID] = &instanceCalls{instance: &instance_model.Instance{Id: instanceID}}
	runtime.calls[callID] = &liveCall{envelope: CallEnvelope{CallID: callID, InstanceID: instanceID, Direction: "inbound", State: CallStateRinging, Version: 1}}
	runtime.instances[instanceID].active = callID
	runtime.active = 1
	runtime.mu.Unlock()
	if _, err := runtime.RejectUnclaimed(instanceID, callID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := runtime.claim(instanceID, callID, fmt.Sprintf("%032x", 1)); err != ErrCallEnded {
		t.Fatalf("claim after reject error = %v, want ended", err)
	}
	if runtime.active != 0 {
		t.Fatalf("active slots = %d, want 0", runtime.active)
	}
}

func TestTURNURLsFromHost(t *testing.T) {
	got := turnURLs("turn.example.test")
	want := []string{"turn:turn.example.test:3478?transport=udp", "turn:turn.example.test:3478?transport=tcp", "turns:turn.example.test:443?transport=tcp"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("turnURLs() = %#v, want %#v", got, want)
	}
}

func TestH264RTPFragmentReassemblesAnnexB(t *testing.T) {
	nalu := append([]byte{0x65}, make([]byte, 200)...)
	fragments := (&codecs.H264Payloader{}).Payload(64, nalu)
	if len(fragments) < 2 {
		t.Fatalf("packetizer returned %d fragments", len(fragments))
	}
	parser := &codecs.H264Packet{}
	var output []byte
	for _, fragment := range fragments {
		decoded, err := parser.Unmarshal(fragment)
		if err != nil {
			t.Fatal(err)
		}
		output = append(output, decoded...)
	}
	want := append([]byte{0, 0, 0, 1}, nalu...)
	if !reflect.DeepEqual(output, want) {
		t.Fatal("H264 FU-A packets did not reassemble to one Annex-B NAL unit")
	}
}

func TestResampleMonoPreservesDuration(t *testing.T) {
	in := make([]float32, 4800)
	for i := range in {
		in[i] = float32(i)
	}
	out := resampleMono(in, 48000, 16000)
	if len(out) != 1600 {
		t.Fatalf("resampled duration = %d samples, want 1600", len(out))
	}
	if out[0] != in[0] || out[len(out)-1] <= out[0] {
		t.Fatal("resampler output did not preserve the input progression")
	}
}

func TestBrowserOpusPacketsAggregateIntoMeowcallerFrames(t *testing.T) {
	var pending []float32
	var frameCount int
	for packet := 0; packet < 3; packet++ {
		frames, remaining := splitAudioFrames(pending, make([]float32, 320), callAudioFrame)
		pending = remaining
		frameCount += len(frames)
	}
	if frameCount != 1 || len(pending) != 0 {
		t.Fatalf("three 20ms browser packets produced %d frames and %d pending samples, want one 60ms frame", frameCount, len(pending))
	}
	frames, remaining := splitAudioFrames(nil, make([]float32, 1600), callAudioFrame)
	if len(frames) != 1 || len(remaining) != 640 {
		t.Fatalf("50ms browser packet split into %d frames and %d pending samples, want one frame and 640 samples", len(frames), len(remaining))
	}
}

func TestVideoFrameDurationFollowsVariableRTPClock(t *testing.T) {
	fallback := time.Second / callVideoFPS
	if got := videoFrameDuration(3000, 0, 90000, fallback); got != time.Second/30 {
		t.Fatalf("30fps duration = %s", got)
	}
	if got := videoFrameDuration(9000, 3000, 90000, fallback); got != 2*time.Second/30 {
		t.Fatalf("variable 15fps interval = %s", got)
	}
	if got := videoFrameDuration(1000, 0xFFFFFF00, 90000, fallback); got <= 0 || got > 250*time.Millisecond {
		t.Fatalf("wrapped RTP timestamp duration = %s", got)
	}
}

func TestBrowserVideoSampleBuilderReordersRTPPackets(t *testing.T) {
	builder := samplebuilder.New(2, &codecs.H264Packet{}, 90000, samplebuilder.WithMaxTimeDelay(250*time.Millisecond))
	for _, packet := range []*rtp.Packet{
		{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 10, Timestamp: 9000}, Payload: []byte{0x65, 0x01}},
		{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 12, Timestamp: 9000, Marker: true}, Payload: []byte{0x61, 0x03}},
		{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 11, Timestamp: 9000}, Payload: []byte{0x61, 0x02}},
	} {
		builder.Push(packet)
	}
	builder.Flush()
	sample := builder.Pop()
	if sample == nil || sample.PacketTimestamp != 9000 {
		t.Fatalf("reordered H264 sample = %+v", sample)
	}
	if !bytes.Contains(sample.Data, []byte{0x61, 0x02}) || !bytes.Contains(sample.Data, []byte{0x61, 0x03}) || bytes.Index(sample.Data, []byte{0x61, 0x02}) > bytes.Index(sample.Data, []byte{0x61, 0x03}) {
		t.Fatalf("sample lost packet payloads: %x", sample.Data)
	}
}

func TestVideoOrientationPublishesQuarterTurns(t *testing.T) {
	call := &liveCall{envelope: CallEnvelope{CallID: "orientation", Version: 2}}
	runtime := NewRuntime(RuntimeOptions{})
	sink := browserVideoSink{runtime: runtime, call: call}
	sink.SetOrientation(3)
	if call.envelope.VideoOrientation == nil || *call.envelope.VideoOrientation != 3 || call.envelope.Version != 3 {
		t.Fatalf("orientation was not published: %+v", call.envelope)
	}
}

func TestPeerDisconnectGraceAndRingTimeout(t *testing.T) {
	runtime := NewRuntime(RuntimeOptions{HeartbeatTTL: time.Minute})
	now := time.Now().UTC()
	runtime.mu.Lock()
	runtime.calls["active"] = &liveCall{envelope: CallEnvelope{CallID: "active", State: CallStateActive, CreatedAt: now.Add(-time.Minute).UnixMilli()}, lastBeat: now, peerLostAt: now.Add(-16 * time.Second)}
	runtime.calls["ring"] = &liveCall{envelope: CallEnvelope{CallID: "ring", State: CallStateRinging, Direction: "inbound", CreatedAt: now.Add(-61 * time.Second).UnixMilli()}}
	runtime.mu.Unlock()
	runtime.expireCalls(now)
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if got := runtime.calls["active"].envelope.EndReason; got != "browser_media_disconnected" {
		t.Fatalf("peer-loss reason = %q", got)
	}
	if got := runtime.calls["ring"].envelope.EndReason; got != "ring_timeout" {
		t.Fatalf("ring reason = %q", got)
	}
}

func TestPionUDPMuxLoopbackCarriesRTP(t *testing.T) {
	probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()

	runtime := NewRuntime(RuntimeOptions{UDPBind: "0.0.0.0", UDPPort: port})
	server, err := runtime.newPeerConnection("test-instance")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	browser, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer browser.Close()
	serverVideo, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "packetization-mode=1;profile-level-id=42e01f"}, "video", "server")
	if err != nil {
		t.Fatal(err)
	}
	serverVideoSender, err := server.AddTrack(serverVideo)
	if err != nil {
		t.Fatal(err)
	}
	peerPLI := make(chan struct{}, 1)
	live := &liveCall{requestPeerKeyframe: func() error { peerPLI <- struct{}{}; return nil }}
	go runtime.drainSenderRTCP(live, serverVideoSender, true)
	videoTrack, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "packetization-mode=1;profile-level-id=42e01f"}, "video", "browser")
	if err != nil {
		t.Fatal(err)
	}
	videoSender, err := browser.AddTrack(videoTrack)
	if err != nil {
		t.Fatal(err)
	}
	gotPLI := make(chan struct{}, 1)
	go func() {
		buffer := make([]byte, 1500)
		for {
			n, _, readErr := videoSender.Read(buffer)
			if readErr != nil {
				return
			}
			packets, unmarshalErr := rtcp.Unmarshal(buffer[:n])
			if unmarshalErr != nil {
				continue
			}
			for _, packet := range packets {
				if _, ok := packet.(*rtcp.PictureLossIndication); ok {
					select {
					case gotPLI <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	videoTimestamps := make(chan uint32, 3)
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "browser")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := browser.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buffer := make([]byte, 1500)
		for {
			if _, _, readErr := sender.Read(buffer); readErr != nil {
				return
			}
		}
	}()
	gotRTP := make(chan struct{}, 1)
	connected := make(chan struct{}, 1)
	browser.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
	})
	server.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if remote.Kind() == webrtc.RTPCodecTypeVideo {
			for {
				packet, _, readErr := remote.ReadRTP()
				if readErr != nil {
					return
				}
				select {
				case videoTimestamps <- packet.Timestamp:
				default:
				}
			}
		}
		if remote.Kind() != webrtc.RTPCodecTypeAudio {
			return
		}
		if _, _, readErr := remote.ReadRTP(); readErr == nil {
			gotRTP <- struct{}{}
		}
	})
	offer, err := browser.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	offerGathered := webrtc.GatheringCompletePromise(browser)
	if err = browser.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-offerGathered:
	case <-time.After(5 * time.Second):
		t.Fatal("browser ICE gathering timed out")
	}
	if err = server.SetRemoteDescription(*browser.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	answer, err := server.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	answerGathered := webrtc.GatheringCompletePromise(server)
	if err = server.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-answerGathered:
	case <-time.After(5 * time.Second):
		t.Fatal("server ICE gathering timed out")
	}
	if err = browser.SetRemoteDescription(*server.LocalDescription()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-connected:
	case <-time.After(8 * time.Second):
		t.Fatalf("Pion loopback did not connect (browser=%s server=%s); browser SDP:\n%s\nserver SDP:\n%s", browser.ConnectionState(), server.ConnectionState(), browser.LocalDescription().SDP, server.LocalDescription().SDP)
	}
	path := pionPathDiagnostics(server)
	if !path.Available || path.LocalCandidateType == "" || path.RemoteCandidateType == "" || path.LocalProtocol == "" || path.RemoteProtocol == "" {
		t.Fatalf("connected loopback did not expose selected Pion path: %+v", path)
	}
	if path.CurrentRoundTripMs != nil && *path.CurrentRoundTripMs <= 0 {
		t.Fatalf("reported nonpositive Pion RTT: %+v", path)
	}
	iceTransport := server.SCTP().Transport().ICETransport()
	selectedPair, pairErr := iceTransport.GetSelectedCandidatePair()
	if pairErr != nil || selectedPair == nil {
		t.Fatalf("Pion did not provide its selected candidate pair: pair=%v err=%v", selectedPair, pairErr)
	}
	withoutRTT := pionPathDiagnosticsFromPair(selectedPair, webrtc.ICECandidatePairStats{}, false)
	if withoutRTT.CurrentRoundTripMs != nil {
		t.Fatalf("default pair stats were reported as a measured RTT: %+v", withoutRTT)
	}
	encodedPath, marshalErr := json.Marshal(path)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var pathFields map[string]json.RawMessage
	if err = json.Unmarshal(encodedPath, &pathFields); err != nil {
		t.Fatal(err)
	}
	for key := range pathFields {
		switch key {
		case "available", "currentRoundTripMs", "localCandidateType", "remoteCandidateType", "localProtocol", "remoteProtocol":
		default:
			t.Fatalf("Pion path exposed unexpected detail field %q: %s", key, encodedPath)
		}
	}
	serverSSRC := uint32(serverVideoSender.GetParameters().Encodings[0].SSRC)
	if err = browser.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: serverSSRC}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peerPLI:
	case <-time.After(3 * time.Second):
		t.Fatal("browser PLI was not drained and forwarded to the WhatsApp feedback adapter")
	}
	for range 3 {
		if err = track.WriteSample(media.Sample{Data: []byte{0xf8, 0xff, 0xfe}, Duration: 20 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	frame := []byte{0, 0, 0, 1, 0x65, 0x88, 0x84, 0x21}
	timestamps := make([]uint32, 0, 3)
	for _, duration := range []time.Duration{time.Second / 30, time.Second / 15, time.Second / 24} {
		if err = videoTrack.WriteSample(media.Sample{Data: frame, Duration: duration}); err != nil {
			t.Fatal(err)
		}
		select {
		case timestamp := <-videoTimestamps:
			timestamps = append(timestamps, timestamp)
		case <-time.After(3 * time.Second):
			t.Fatal("Pion loopback did not deliver an H264 timestamp")
		}
	}
	if timestamps[1]-timestamps[0] != 2999 || timestamps[2]-timestamps[1] != 6000 {
		t.Fatalf("H264 RTP timestamp deltas = %d, %d; want 2999, 6000", timestamps[1]-timestamps[0], timestamps[2]-timestamps[1])
	}
	ssrc := uint32(videoSender.GetParameters().Encodings[0].SSRC)
	if err = server.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gotPLI:
	case <-time.After(3 * time.Second):
		t.Fatal("browser sender did not receive forwarded keyframe request")
	}
	select {
	case <-gotRTP:
	case <-time.After(3 * time.Second):
		t.Fatal("Pion loopback delivered no RTP packet")
	}
}

func TestOpusRTPReorderShuffledDuplicateAndLostPacket(t *testing.T) {
	base := time.Unix(100, 0)
	buffer := newOpusRTPReorder(maxAudioReorderPackets)
	packet := func(sequence uint16, timestamp uint32) *rtp.Packet {
		return &rtp.Packet{Header: rtp.Header{SequenceNumber: sequence, Timestamp: timestamp}, Payload: []byte{0xf8, 0xff, 0xfe}}
	}
	if got := buffer.Push(packet(10, 0), base); len(got) != 1 || got[0].SequenceNumber != 10 {
		t.Fatalf("first packet release = %#v", got)
	}
	if got := buffer.Push(packet(12, 1920), base.Add(time.Millisecond)); len(got) != 0 {
		t.Fatalf("out-of-order packet released before missing packet: %#v", got)
	}
	if got := buffer.Push(packet(11, 960), base.Add(2*time.Millisecond)); len(got) != 2 || got[0].SequenceNumber != 11 || got[1].SequenceNumber != 12 {
		t.Fatalf("reordered sequence = %#v, want [11, 12]", got)
	}
	if got := buffer.Push(packet(11, 960), base.Add(3*time.Millisecond)); len(got) != 0 {
		t.Fatalf("duplicate/late packet was not discarded: %#v", got)
	}
	if got := buffer.Push(packet(14, 3840), base.Add(4*time.Millisecond)); len(got) != 0 {
		t.Fatalf("packet after loss released before jitter deadline: %#v", got)
	}
	got := buffer.Flush(base.Add(audioReorderDelay + 5*time.Millisecond))
	if len(got) != 2 || got[0] != nil || got[1].SequenceNumber != 14 {
		t.Fatalf("loss release = %#v, want [PLC gap, sequence 14]", got)
	}
	if int32(got[1].Timestamp-uint32(2880)) != 960 {
		t.Fatalf("lost packet timestamp gap = %d, want 960 ticks at 48 kHz", int32(got[1].Timestamp-uint32(2880)))
	}
}

func ptrJID(jid types.JID) *types.JID { return &jid }
