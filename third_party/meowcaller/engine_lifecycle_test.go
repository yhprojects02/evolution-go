package meowcaller

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type lifecycleAudioSource struct {
	closeCount int
}

func (s *lifecycleAudioSource) ReadFrame() ([]float32, error) {
	return nil, errors.New("unused")
}

func (s *lifecycleAudioSource) Close() error {
	s.closeCount++
	return nil
}

type lifecycleAudioSink struct {
	closeCount int
}

func (s *lifecycleAudioSink) WriteFrame([]float32) error {
	return nil
}

func (s *lifecycleAudioSink) Close() error {
	s.closeCount++
	return nil
}

func testEngineWithOutgoingCall() (*engine, *Call) {
	c := &Client{}
	c.eng = newEngine(c)
	call := &Call{eng: c.eng, id: "CID", peer: peerJID(), phase: CallPhaseCalling}
	c.eng.calls["CID"] = &engineCall{
		call:        call,
		direction:   CallDirectionOutgoing,
		from:        peerJID(),
		creator:     creatorJID(),
		localVideo:  true,
		remoteVideo: true,
	}
	return c.eng, call
}

func testEngineWithIncomingCall() (*engine, *Call, types.JID, types.JID) {
	ownPN := types.NewJID("60183568788", types.DefaultUserServer)
	ownLID := types.NewJID("self-lid", types.HiddenUserServer)
	device := &store.Device{ID: &ownPN, LID: ownLID}
	c := &Client{wa: whatsmeow.NewClient(device, nil)}
	c.eng = newEngine(c)
	call := &Call{eng: c.eng, id: "CID", peer: peerJID(), phase: CallPhaseRinging}
	c.eng.calls["CID"] = &engineCall{call: call, direction: CallDirectionIncoming, from: peerJID(), creator: creatorJID()}
	return c.eng, call, ownPN, ownLID
}

func TestOwnCallJIDMatchesStoredPhoneAndLIDWithoutDeviceSuffix(t *testing.T) {
	eng, _, ownPN, ownLID := testEngineWithIncomingCall()
	phoneDevice := ownPN
	phoneDevice.Device = 4
	lidDevice := ownLID
	lidDevice.Device = 9
	for name, candidate := range map[string]types.JID{"phone": phoneDevice, "lid": lidDevice} {
		t.Run(name, func(t *testing.T) {
			if !eng.isOwnCallJID(candidate) {
				t.Fatalf("own %s JID with device suffix did not match", name)
			}
		})
	}
	if eng.isOwnCallJID(peerJID()) {
		t.Fatal("foreign peer JID matched the local account")
	}
	if eng.isOwnCallJID(types.EmptyJID) {
		t.Fatal("empty JID matched the local account")
	}
}

func TestIncomingSelfAcceptFinishesLocallyAndMakesStaleControlsNoOps(t *testing.T) {
	eng, call, _, ownLID := testEngineWithIncomingCall()
	sent := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error { sent++; return nil }
	var endReason string
	call.OnEnd(func(reason string) { endReason = reason })
	acceptingDevice := ownLID
	acceptingDevice.Device = 7
	eng.onAccept(&events.CallAccept{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: acceptingDevice}})

	if call.State() != CallPhaseEnded || endReason != "answered_elsewhere" {
		t.Fatalf("self accept ended (%v, %q), want Ended/answered_elsewhere", call.State(), endReason)
	}
	if eng.lookup(call.ID()) != nil {
		t.Fatal("answered-elsewhere call remained registered")
	}
	if err := call.Hangup(); err != nil {
		t.Fatalf("stale hangup: %v", err)
	}
	if err := call.Reject(); err != nil {
		t.Fatalf("stale reject: %v", err)
	}
	if sent != 0 {
		t.Fatalf("local sibling acceptance sent %d signaling nodes, want none", sent)
	}
}

func TestIncomingForeignAcceptIsIgnoredAndBrowserAnswerWins(t *testing.T) {
	t.Run("foreign acceptance", func(t *testing.T) {
		eng, call, _, _ := testEngineWithIncomingCall()
		eng.onAccept(&events.CallAccept{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: peerJID()}})
		if eng.lookup(call.ID()) == nil || call.State() != CallPhaseRinging {
			t.Fatal("foreign accept ended the incoming call")
		}
	})
	t.Run("browser claim", func(t *testing.T) {
		for _, phase := range []CallPhase{CallPhaseConnecting, CallPhaseActive} {
			t.Run(strconv.Itoa(int(phase)), func(t *testing.T) {
				eng, call, _, ownLID := testEngineWithIncomingCall()
				if err := call.Answer(); err != nil {
					t.Fatalf("browser answer: %v", err)
				}
				m := eng.lookup(call.ID())
				// Model the first mute_v2 arriving and the deferred <accept> being sent.
				eng.mu.Lock()
				m.acceptPending = false
				eng.mu.Unlock()
				call.setPhase(phase)
				acceptingDevice := ownLID
				acceptingDevice.Device = 7
				eng.onAccept(&events.CallAccept{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: acceptingDevice}})
				if eng.lookup(call.ID()) != m || m.acceptPending || !m.browserClaimed || call.State() != phase {
					t.Fatalf("sibling acceptance displaced browser-owned call after pending accept, phase=%v", phase)
				}
			})
		}
	})
}

func TestIncomingBrowserAnswerRacesSelfAcceptAtomically(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		eng, call, _, ownLID := testEngineWithIncomingCall()
		var endReason string
		call.OnEnd(func(reason string) { endReason = reason })
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _ = call.Answer() }()
		go func() {
			defer wg.Done()
			<-start
			eng.onAccept(&events.CallAccept{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: ownLID}})
		}()
		close(start)
		wg.Wait()
		if m := eng.lookup(call.ID()); m != nil {
			if !m.acceptPending || call.State() != CallPhaseConnecting {
				t.Fatalf("attempt %d: retained call is not browser-claimed", attempt)
			}
		} else if call.State() != CallPhaseEnded || endReason != "answered_elsewhere" {
			t.Fatalf("attempt %d: removed call ended as (%v, %q)", attempt, call.State(), endReason)
		}
	}
}

func TestConcurrentSelfAcceptAndTimeoutHangupSendAtMostOneTerminate(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		eng, call, _, ownLID := testEngineWithIncomingCall()
		var sent atomic.Int32
		eng.sendCallNode = func(context.Context, waBinary.Node) error { sent.Add(1); return nil }
		var endReason string
		call.OnEnd(func(reason string) { endReason = reason })
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _ = call.Hangup() }()
		go func() {
			defer wg.Done()
			<-start
			eng.onAccept(&events.CallAccept{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: ownLID}})
		}()
		close(start)
		wg.Wait()
		count := sent.Load()
		if endReason == "answered_elsewhere" && count != 0 {
			t.Fatalf("attempt %d: answered-elsewhere cleanup sent %d nodes", attempt, count)
		}
		if endReason == "hangup" && count != 1 {
			t.Fatalf("attempt %d: timeout hangup sent %d nodes, want one", attempt, count)
		}
		if endReason != "answered_elsewhere" && endReason != "hangup" {
			t.Fatalf("attempt %d: unexpected end reason %q", attempt, endReason)
		}
	}
}

func videoStateNode(state int) *waBinary.Node {
	return &waBinary.Node{Tag: "video", Attrs: waBinary.Attrs{
		"call-id": "CID", "state": strconv.Itoa(state),
	}}
}

func senderVideoState(sender *videoSender) (active, gated bool) {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	return sender.active, sender.sendGated
}

func TestCallVideoUpgradeGatesUntilPeerAcceptAndCanStop(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.videoTx = &videoSender{}
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return nil
	}

	if err := call.StartVideo(); err != nil {
		t.Fatalf("StartVideo: %v", err)
	}
	if len(sent) != 1 || sent[0].GetChildren()[0].AttrGetter().Int("state") != signaling.VideoStateUpgradeRequestV2 {
		t.Fatalf("StartVideo sent %#v, want one state=11 stanza", sent)
	}
	if orientation, ok := sent[0].GetChildren()[0].Attrs["device_orientation"]; !ok || orientation != "0" {
		t.Fatalf("StartVideo orientation = %v, want explicit 0", orientation)
	}
	if active, gated := senderVideoState(m.videoTx); !active || !gated {
		t.Fatalf("upgrade sender state = active:%v gated:%v, want true,true", active, gated)
	}

	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeAccept))
	if len(sent) != 2 || sent[1].GetChildren()[0].AttrGetter().Int("state") != signaling.VideoStateEnabled {
		t.Fatalf("peer acceptance sent %#v, want state=1 announcement after state=4", sent)
	}
	if orientation, ok := sent[1].GetChildren()[0].Attrs["device_orientation"]; !ok || orientation != "0" {
		t.Fatalf("enabled announcement orientation = %v, want explicit 0", orientation)
	}
	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("accepted sender state = active:%v gated:%v, want true,false", active, gated)
	}

	if err := call.StopVideo(); err != nil {
		t.Fatalf("StopVideo: %v", err)
	}
	if len(sent) != 3 || sent[2].GetChildren()[0].AttrGetter().Int("state") != signaling.VideoStateStopped {
		t.Fatalf("StopVideo sent %#v, want trailing state=6 stanza", sent)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("video sender remained active after StopVideo")
	}
	if call.IsSendingVideo() {
		t.Fatal("call remained marked as sending video after StopVideo")
	}
	if !call.IsReceivingVideo() || !call.IsVideo() {
		t.Fatal("stopping local video also stopped the peer video flow")
	}
}

func TestSetVideoEnabledOnlyTogglesLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.videoTx = &videoSender{active: true}
	var states []int
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		return nil
	}

	if err := call.SetVideoEnabled(false); err != nil {
		t.Fatalf("SetVideoEnabled(false): %v", err)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("disabling local video left sender active")
	}
	if call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("disabled state = send:%v receive:%v, want false,true", call.IsSendingVideo(), call.IsReceivingVideo())
	}

	if err := call.SetVideoEnabled(true); err != nil {
		t.Fatalf("SetVideoEnabled(true): %v", err)
	}
	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("enabled sender state = active:%v gated:%v, want true,false", active, gated)
	}
	if !call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("enabled state = send:%v receive:%v, want true,true", call.IsSendingVideo(), call.IsReceivingVideo())
	}
	if len(states) != 2 || states[0] != signaling.VideoStateDisabled || states[1] != signaling.VideoStateEnabled {
		t.Fatalf("toggle states = %v, want [0 1]", states)
	}
}

func TestCallAcceptVideoPreservesDisabledLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.peerVideoUpgrade = true
	m.videoTx = &videoSender{}
	var states []int
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		return nil
	}

	if err := call.AcceptVideo(); err != nil {
		t.Fatalf("AcceptVideo: %v", err)
	}
	if len(states) != 2 || states[0] != signaling.VideoStateStopped || states[1] != signaling.VideoStateUpgradeAccept {
		t.Fatalf("AcceptVideo states = %v, want [6 4]", states)
	}
	if active, gated := senderVideoState(m.videoTx); active || gated {
		t.Fatalf("accepted sender state = active:%v gated:%v, want false,false", active, gated)
	}
	if call.IsSendingVideo() {
		t.Fatal("accepting peer video enabled the local sender")
	}
}

func TestCallAcceptVideoPreservesEnabledLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerVideoUpgrade = true
	m.videoTx = &videoSender{active: true}
	var states []int
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		return nil
	}

	if err := call.AcceptVideo(); err != nil {
		t.Fatalf("AcceptVideo: %v", err)
	}
	if len(states) != 1 || states[0] != signaling.VideoStateUpgradeAccept {
		t.Fatalf("AcceptVideo states = %v, want [4]", states)
	}
	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("accepted sender state = active:%v gated:%v, want true,false", active, gated)
	}
}

func TestInboundVideoStopOnlyDisablesRemoteFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.videoTx = &videoSender{active: true}
	var got VideoState
	call.OnVideoState(func(state VideoState) { got = state })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateStopped))

	if got.Raw != signaling.VideoStateStopped {
		t.Fatalf("video callback state = %d, want 6", got.Raw)
	}
	if active, _ := senderVideoState(m.videoTx); !active {
		t.Fatal("peer stopping video disabled the local sender")
	}
	if !call.IsSendingVideo() || call.IsReceivingVideo() || !call.IsVideo() {
		t.Fatalf("directional state after peer stop = send:%v receive:%v any:%v, want true,false,true",
			call.IsSendingVideo(), call.IsReceivingVideo(), call.IsVideo())
	}
}

func TestInboundVideoEnabledDoesNotRestartLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.remoteVideo = false
	m.videoTx = &videoSender{}

	eng.onVideoStanza(videoStateNode(signaling.VideoStateEnabled))

	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("peer enabling video restarted the local sender")
	}
	if call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("directional state after peer enable = send:%v receive:%v, want false,true",
			call.IsSendingVideo(), call.IsReceivingVideo())
	}
}

func TestInboundVideoEnabledAcceptsPendingLocalUpgrade(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = true
	m.remoteVideo = false
	m.videoGate = true
	m.videoTx = &videoSender{active: true, sendGated: true}
	var keyframeRequests int
	call.OnVideoKeyframeRequest(func() { keyframeRequests++ })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateEnabled))

	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("pending sender after peer enabled = active:%v gated:%v, want true,false", active, gated)
	}
	if !call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("video flows after peer enabled = send:%v receive:%v, want true,true",
			call.IsSendingVideo(), call.IsReceivingVideo())
	}
	if keyframeRequests != 1 {
		t.Fatalf("keyframe requests = %d, want 1", keyframeRequests)
	}
}

func TestInboundVideoUpgradeWaitsForExplicitAcceptance(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.videoTx = &videoSender{}
	var sent int
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		sent++
		return nil
	}
	var got VideoState
	call.OnVideoState(func(state VideoState) { got = state })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeRequestV2))

	if !got.Upgrade || got.Raw != signaling.VideoStateUpgradeRequestV2 {
		t.Fatalf("video callback = %+v, want pending state 11", got)
	}
	if sent != 0 {
		t.Fatalf("inbound upgrade auto-sent %d signaling stanzas", sent)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("inbound upgrade activated video before explicit acceptance")
	}
	if !m.peerVideoUpgrade {
		t.Fatal("inbound upgrade was not marked pending")
	}
}

func TestVideoAcceptanceRequestsSourceKeyframe(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].videoTx = &videoSender{}
	eng.sendCallNode = func(context.Context, waBinary.Node) error { return nil }
	var requests int
	call.OnVideoKeyframeRequest(func() { requests++ })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeAccept))

	if requests != 1 {
		t.Fatalf("keyframe requests = %d, want 1", requests)
	}
}

func TestCallSetsVideoOrientation(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	var sent waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = node
		return nil
	}

	if err := call.SetVideoOrientation(2); err != nil {
		t.Fatalf("SetVideoOrientation: %v", err)
	}
	video := sent.GetChildren()[0].AttrGetter()
	if video.Int("state") != signaling.VideoStateEnabled || video.Int("device_orientation") != 2 {
		t.Fatalf("orientation stanza attrs = %#v", sent.GetChildren()[0].Attrs)
	}
	if err := call.SetVideoOrientation(4); err == nil {
		t.Fatal("SetVideoOrientation accepted orientation 4")
	}
}

func TestHangupTearsDownLocallyWhenSignalingFails(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	var canceled bool
	eng.calls[call.ID()].cancel = func() { canceled = true }
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		return errors.New("network unavailable")
	}
	var reason string
	call.OnEnd(func(got string) { reason = got })

	err := call.Hangup()

	if err == nil {
		t.Fatal("Hangup returned nil after signaling failure")
	}
	if !canceled {
		t.Fatal("Hangup did not cancel local media")
	}
	if call.State() != CallPhaseEnded || reason != "hangup" {
		t.Fatalf("local end state = (%d, %q), want (Ended, hangup)", call.State(), reason)
	}
	if eng.lookup(call.ID()) != nil {
		t.Fatal("Hangup retained ended call in engine registry")
	}
}

func TestOutgoingPeerAcceptLifecycle(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	from := types.JID{User: "222222222222222", Server: types.HiddenUserServer}

	eng.onPreAccept(&events.CallPreAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: from},
	})
	if got := call.State(); got != CallPhaseRinging {
		t.Fatalf("after preaccept phase = %d, want Ringing", got)
	}

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: from},
		Data:          &waBinary.Node{Tag: "accept"},
	})
	if got := call.State(); got != CallPhaseConnecting {
		t.Fatalf("after accept phase = %d, want Connecting", got)
	}
}

func TestOutgoingAcceptRekeysToAnsweringDevice(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerLID = peerJID().String()
	answeringDevice := peerJID()
	answeringDevice.Device = 7
	var rekeyed string
	m.rekeyPeer = func(peer string) error {
		rekeyed = peer
		return nil
	}

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: answeringDevice},
		Data:          &waBinary.Node{Tag: "accept"},
	})

	if rekeyed != answeringDevice.String() {
		t.Fatalf("rekeyed peer = %q, want %q", rekeyed, answeringDevice.String())
	}
	eng.mu.Lock()
	gotPeer, gotFrom := m.peerLID, m.from
	eng.mu.Unlock()
	if gotPeer != answeringDevice.String() || gotFrom != answeringDevice {
		t.Fatalf("accepted routing = (%q, %s), want (%q, %s)", gotPeer, gotFrom, answeringDevice.String(), answeringDevice)
	}
}

func TestParseRelayDataResolvesElectedPeerDevice(t *testing.T) {
	primary := peerJID()
	companion := peerJID()
	companion.Device = 7
	relay := &waBinary.Node{
		Tag:   "relay",
		Attrs: waBinary.Attrs{"peer_pid": "2", "self_pid": "1"},
		Content: []waBinary.Node{
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "0", "jid": primary}},
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "2", "jid": companion}},
		},
	}

	rd := parseRelayData(relay)

	if rd.peerJID != companion {
		t.Fatalf("relay peer = %s, want %s", rd.peerJID, companion)
	}
}

func TestRelayRekeysToElectedPeerDevice(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerLID = peerJID().String()
	companion := peerJID()
	companion.Device = 7
	var rekeyed string
	m.rekeyPeer = func(peer string) error {
		rekeyed = peer
		return nil
	}
	relay := &waBinary.Node{
		Tag:   "relay",
		Attrs: waBinary.Attrs{"peer_pid": "2"},
		Content: []waBinary.Node{
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "2", "jid": companion}},
		},
	}

	eng.onRelay(call.ID(), relay)

	if rekeyed != companion.String() {
		t.Fatalf("rekeyed peer = %q, want %q", rekeyed, companion.String())
	}
	eng.mu.Lock()
	gotPeer := m.peerLID
	eng.mu.Unlock()
	if gotPeer != companion.String() {
		t.Fatalf("stored peer = %q, want %q", gotPeer, companion.String())
	}
}

func TestUnqualifiedAcceptPreservesRelayElectedPeerDevice(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerLID = peerJID().String()
	companion := peerJID()
	companion.Device = 7
	var rekeyed []string
	m.rekeyPeer = func(peer string) error {
		rekeyed = append(rekeyed, peer)
		return nil
	}
	relay := &waBinary.Node{
		Tag:   "relay",
		Attrs: waBinary.Attrs{"peer_pid": "2"},
		Content: []waBinary.Node{
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "2", "jid": companion}},
		},
	}
	eng.onRelay(call.ID(), relay)

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: peerJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})

	if len(rekeyed) != 1 || rekeyed[0] != companion.String() {
		t.Fatalf("rekeyed peers = %v, want only %q", rekeyed, companion.String())
	}
	eng.mu.Lock()
	gotPeer := m.peerLID
	eng.mu.Unlock()
	if gotPeer != companion.String() {
		t.Fatalf("stored peer after accept = %q, want %q", gotPeer, companion.String())
	}
}

func TestOutgoingPeerAcceptCallbackFiresOnceAfterMediaStarted(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseConnecting)
	var accepted int
	call.OnPeerAccept(func() { accepted++ })
	event := &events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: peerJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	}

	eng.onAccept(event)
	eng.onAccept(event)

	if accepted != 1 {
		t.Fatalf("peer accept callbacks = %d, want 1", accepted)
	}
}

func TestOutgoingPeerAcceptCallbackReplaysAfterRegistration(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: peerJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})
	var accepted int

	call.OnPeerAccept(func() { accepted++ })

	if accepted != 1 {
		t.Fatalf("late peer accept callbacks = %d, want 1", accepted)
	}
}

func TestOutgoingPeerAcceptIgnoredAfterCallEnded(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseEnded)
	var accepted int
	call.OnPeerAccept(func() { accepted++ })

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: peerJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})

	if accepted != 0 {
		t.Fatalf("peer accept callbacks after end = %d, want 0", accepted)
	}
}

func TestOutgoingPeerAcceptDoesNotRegressActiveCall(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseActive)

	eng.onPreAccept(&events.CallPreAccept{BasicCallMeta: types.BasicCallMeta{CallID: "CID"}})
	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID"},
		Data:          &waBinary.Node{Tag: "accept"},
	})
	if got := call.State(); got != CallPhaseActive {
		t.Fatalf("phase = %d, want Active", got)
	}
}

func TestPeerRejectEndsCall(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	var reason string
	call.OnEnd(func(r string) { reason = r })

	eng.onReject(&events.CallReject{BasicCallMeta: types.BasicCallMeta{CallID: "CID"}})
	if got := call.State(); got != CallPhaseEnded {
		t.Fatalf("phase = %d, want Ended", got)
	}
	if reason != "rejected" {
		t.Fatalf("reason = %q, want rejected", reason)
	}
}

func TestFinishCallClosesAttachedAudioDevices(t *testing.T) {
	source := &lifecycleAudioSource{}
	sink := &lifecycleAudioSink{}
	call := &Call{id: "CALL", phase: CallPhaseActive}
	call.Play(source)
	call.Receive(sink)
	eng := &engine{calls: map[string]*engineCall{
		"CALL": {call: call},
	}}

	eng.finishCall("CALL", "done")

	if source.closeCount != 1 {
		t.Fatalf("audio source close count = %d, want 1", source.closeCount)
	}
	if sink.closeCount != 1 {
		t.Fatalf("audio sink close count = %d, want 1", sink.closeCount)
	}
}
