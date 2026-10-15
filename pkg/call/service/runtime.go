package call_service

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	"github.com/google/uuid"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

const (
	CallStateDialing    = "dialing"
	CallStateRinging    = "ringing"
	CallStateConnecting = "connecting"
	CallStateActive     = "active"
	CallStateEnded      = "ended"
	MediaAudio          = "audio"
	MediaVideo          = "video"
	MaxConcurrentCalls  = 4
)

var (
	ErrDisabled           = errors.New("calls are disabled")
	ErrNotFound           = errors.New("call not found")
	ErrOwner              = errors.New("call controller is not authorized")
	ErrCallBusy           = errors.New("instance already has an active call")
	ErrCallCapacity       = errors.New("call capacity reached")
	ErrCallEnded          = errors.New("call has ended")
	ErrCallConflict       = errors.New("call is already controlled by another client")
	ErrHeartbeatExpired   = errors.New("call controller heartbeat expired")
	ErrCallPeerNotAllowed = errors.New("call peer is not in the calls allowlist")
	ErrCallDrainConflict  = errors.New("call deployment drain is owned by another release")
)

var phoneDigits = regexp.MustCompile(`\D`)

type CallEnvelope struct {
	CallID           string `json:"callId"`
	InstanceID       string `json:"instanceId"`
	Peer             string `json:"peer"`
	Direction        string `json:"direction"`
	Media            string `json:"media"`
	State            string `json:"state"`
	Version          uint64 `json:"version"`
	CreatedAt        int64  `json:"createdAt"`
	UpdatedAt        int64  `json:"updatedAt"`
	ConnectedAt      int64  `json:"connectedAt,omitempty"`
	EndedAt          int64  `json:"endedAt,omitempty"`
	VideoOrientation *int   `json:"videoOrientation,omitempty"`
	EndReason        string `json:"endReason,omitempty"`
	ControllerID     string `json:"controllerId,omitempty"`
}

type RuntimeOptions struct {
	Enabled      bool
	Allowlist    []string
	MaxActive    int
	UDPBind      string
	UDPPort      int
	PublicIP     string
	TURNURL      string
	TURNSecret   string
	TURNTTL      time.Duration
	HeartbeatTTL time.Duration
	Publish      func(*instance_model.Instance, CallEnvelope)
}

type DrainRequest struct {
	Owner  string `json:"owner" binding:"required"`
	Action string `json:"action" binding:"required"`
}

type DrainStatus struct {
	Owner    string `json:"owner,omitempty"`
	Acquired bool   `json:"acquired"`
	Active   int    `json:"active"`
	Pending  int    `json:"pending"`
}

type Runtime struct {
	mu         sync.Mutex
	instances  map[string]*instanceCalls
	calls      map[string]*liveCall
	phoneCalls map[string]string
	options    RuntimeOptions
	allow      map[string]struct{}
	active     int
	mux        *ice.UDPMuxDefault
	udpConn    *net.UDPConn
	closed     bool
	drainOwner string
}

type instanceCalls struct {
	instance     *instance_model.Instance
	wa           *whatsmeow.Client
	caller       *meowcaller.Client
	accountPhone string
	active       string
	idempotency  map[string]string
}

type liveCall struct {
	mu                  sync.Mutex
	codecMu             sync.Mutex
	envelope            CallEnvelope
	call                *meowcaller.Call
	accountPhone        string
	owner               string
	claimID             string
	lastBeat            time.Time
	peerLostAt          time.Time
	peerStartAt         time.Time
	waReady             bool
	peerReady           bool
	pc                  *webrtc.PeerConnection
	audioTrack          *webrtc.TrackLocalStaticSample
	videoTrack          *webrtc.TrackLocalStaticSample
	videoSSRC           uint32
	videoTimestamp      uint32
	hasVideoTimestamp   bool
	mediaAttached       bool
	pcOfferPending      bool
	audioCodec          callAudioEncoder
	audioIn             chan []float32
	closeOnce           sync.Once
	answerDone          chan struct{}
	answerFn            func() error // set only by unit tests; production uses call.Answer
	requestPeerKeyframe func() error
}

func NewRuntime(options RuntimeOptions) *Runtime {
	if options.MaxActive <= 0 || options.MaxActive > MaxConcurrentCalls {
		options.MaxActive = MaxConcurrentCalls
	}
	if options.HeartbeatTTL <= 0 {
		options.HeartbeatTTL = 45 * time.Second
	}
	if options.UDPBind == "" {
		options.UDPBind = "0.0.0.0"
	}
	if options.UDPPort <= 0 || options.UDPPort > 65535 {
		options.UDPPort = 40000
	}
	if options.TURNTTL <= 0 {
		options.TURNTTL = 10 * time.Minute
	}
	allow := make(map[string]struct{}, len(options.Allowlist))
	for _, id := range options.Allowlist {
		if id = strings.TrimSpace(id); id != "" {
			if id == "*" {
				allow[id] = struct{}{}
			} else {
				allow[phoneDigits.ReplaceAllString(id, "")] = struct{}{}
			}
		}
	}
	return &Runtime{instances: make(map[string]*instanceCalls), calls: make(map[string]*liveCall), phoneCalls: make(map[string]string), options: options, allow: allow}
}

func (r *Runtime) EnabledFor(instanceID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.options.Enabled || r.closed {
		return false
	}
	return r.localAllowedLocked(r.instances[instanceID])
}

// RefreshInstancePhone updates identity cached during client registration once
// QR pairing or a device session has populated Store.ID.
func (r *Runtime) RefreshInstancePhone(instanceID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.instances[instanceID]
	if entry == nil {
		return false
	}
	phone := canonicalAccountPhone(entry.wa)
	if phone == "" {
		return false
	}
	entry.accountPhone = phone
	return true
}

func (r *Runtime) localAllowedLocked(entry *instanceCalls) bool {
	if len(r.allow) == 0 || entry == nil || entry.accountPhone == "" {
		return false
	}
	return r.phoneAllowed(entry.accountPhone)
}

func (r *Runtime) peerAllowed(peer string) bool {
	user := strings.Split(strings.Split(peer, "@")[0], ":")[0]
	return r.phoneAllowed(user)
}

func (r *Runtime) phoneAllowed(phone string) bool {
	if _, ok := r.allow["*"]; ok {
		return phone != ""
	}
	_, ok := r.allow[phoneDigits.ReplaceAllString(phone, "")]
	return phone != "" && ok
}

func canonicalAccountPhone(wa *whatsmeow.Client) string {
	if wa == nil || wa.Store == nil || wa.Store.ID == nil {
		return ""
	}
	return canonicalPhoneJID(wa, *wa.Store.ID)
}

func canonicalPhoneJID(wa *whatsmeow.Client, jid types.JID) string {
	if jid.Server == types.LegacyUserServer {
		jid.Server = types.DefaultUserServer
	}
	if jid.Server == types.HiddenUserServer || jid.Server == types.HostedLIDServer {
		if wa == nil || wa.Store == nil || wa.Store.LIDs == nil {
			return ""
		}
		pn, err := wa.Store.LIDs.GetPNForLID(context.Background(), jid.ToNonAD())
		if err != nil || (pn.Server != types.DefaultUserServer && pn.Server != types.LegacyUserServer) {
			return ""
		}
		jid = pn
	}
	if jid.Server != types.DefaultUserServer {
		return ""
	}
	return phoneDigits.ReplaceAllString(jid.User, "")
}

func (r *Runtime) resolvePeer(instanceID, raw string) (string, string, error) {
	if raw == "" {
		return "", "", errors.New("peer is required")
	}
	if !strings.Contains(raw, "@") {
		raw += "@" + types.DefaultUserServer
	}
	jid, err := types.ParseJID(raw)
	if err != nil {
		return "", "", errors.New("peer must be a WhatsApp phone or resolvable LID")
	}
	if jid.Server == types.LegacyUserServer {
		jid.Server = types.DefaultUserServer
	}
	if jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer && jid.Server != types.HostedLIDServer {
		return "", "", errors.New("calls support one-to-one phone or LID contacts only")
	}
	r.mu.Lock()
	entry := r.instances[instanceID]
	var wa *whatsmeow.Client
	if entry != nil {
		wa = entry.wa
	}
	r.mu.Unlock()
	phone := canonicalPhoneJID(wa, jid)
	if phone == "" {
		return "", "", errors.New("peer LID has no resolvable phone mapping")
	}
	return jid.ToNonAD().String(), phone, nil
}

// SetPublisher wires call-state events after the WhatsApp service is constructed.
// It is expected to be called once before clients start.
func (r *Runtime) SetPublisher(publish func(*instance_model.Instance, CallEnvelope)) {
	r.mu.Lock()
	r.options.Publish = publish
	r.mu.Unlock()
}

type ICEConfig struct {
	ICEServers []webrtc.ICEServer `json:"iceServers"`
	ExpiresAt  time.Time          `json:"expiresAt,omitempty"`
}

// BrowserICEConfig returns TURN credentials scoped to one instance and short expiry.
func (r *Runtime) BrowserICEConfig(instanceID string) (ICEConfig, error) {
	if err := r.checkEnabled(instanceID); err != nil {
		return ICEConfig{}, err
	}
	config := ICEConfig{}
	urls := turnURLs(r.options.TURNURL)
	if len(urls) == 0 {
		return config, nil
	}
	if r.options.TURNSecret == "" {
		return ICEConfig{}, errors.New("TURN is configured without a REST secret")
	}
	username, credential, expiry, err := r.turnCredentials(instanceID)
	if err != nil {
		return ICEConfig{}, err
	}
	config.ICEServers = []webrtc.ICEServer{{URLs: urls, Username: username, Credential: credential}}
	config.ExpiresAt = expiry
	return config, nil
}

func turnURLs(hostOrURLs string) []string {
	value := strings.TrimSpace(hostOrURLs)
	if value == "" {
		return nil
	}
	if strings.Contains(value, "://") || strings.HasPrefix(value, "turn:") || strings.HasPrefix(value, "turns:") {
		parts := strings.Split(value, ",")
		urls := make([]string, 0, len(parts))
		for _, part := range parts {
			if part = strings.TrimSpace(part); part != "" {
				urls = append(urls, part)
			}
		}
		return urls
	}
	host := strings.TrimPrefix(value, "//")
	return []string{"turn:" + host + ":3478?transport=udp", "turn:" + host + ":3478?transport=tcp", "turns:" + host + ":443?transport=tcp"}
}

func (r *Runtime) turnCredentials(instanceID string) (string, string, time.Time, error) {
	nonce, err := randomID()
	if err != nil {
		return "", "", time.Time{}, err
	}
	expires := time.Now().UTC().Add(r.options.TURNTTL).Truncate(time.Second)
	username := fmt.Sprintf("%d:%s:%s", expires.Unix(), instanceID, nonce)
	mac := hmac.New(sha1.New, []byte(r.options.TURNSecret))
	if _, err = mac.Write([]byte(username)); err != nil {
		return "", "", time.Time{}, err
	}
	return username, base64.StdEncoding.EncodeToString(mac.Sum(nil)), expires, nil
}

// RegisterInstance installs the meowcaller event hooks before the whatsmeow client connects.
func (r *Runtime) RegisterInstance(instance *instance_model.Instance, wa *whatsmeow.Client) error {
	if instance == nil || wa == nil {
		return errors.New("call runtime requires an instance and WhatsApp client")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("call runtime is closed")
	}
	entry := &instanceCalls{instance: instance, wa: wa, accountPhone: canonicalAccountPhone(wa), idempotency: make(map[string]string)}
	// Do not install meowcaller's raw call hooks in a disabled rollout: it
	// pre-accepts incoming offers before the application can decide what to do.
	if r.options.Enabled {
		entry.caller = meowcaller.NewClient(wa)
		entry.caller.OnIncomingCall(func(call *meowcaller.Call) { r.addIncoming(instance.Id, call) })
	}
	var oldCalls []*liveCall
	if previous := r.instances[instance.Id]; previous != nil {
		oldCalls = r.cleanupInstanceLocked(instance.Id, previous, "client_replaced")
	}
	r.instances[instance.Id] = entry
	r.mu.Unlock()
	for _, call := range oldCalls {
		if call.call != nil {
			go call.call.Hangup()
		}
		call.closeMedia()
		r.publish(call)
	}
	return nil
}

func (r *Runtime) Dial(ctx context.Context, instanceID, peer, media, idempotencyKey string) (CallEnvelope, error) {
	if err := r.checkEnabled(instanceID); err != nil {
		return CallEnvelope{}, err
	}
	if len(idempotencyKey) < 32 {
		return CallEnvelope{}, errors.New("idempotencyKey must be a random capability of at least 128 bits")
	}
	if media != MediaAudio && media != MediaVideo {
		return CallEnvelope{}, errors.New("media must be audio or video")
	}
	if strings.TrimSpace(peer) == "" {
		return CallEnvelope{}, errors.New("peer is required")
	}
	peer, peerPhone, parseErr := r.resolvePeer(instanceID, strings.TrimSpace(peer))
	if parseErr != nil {
		return CallEnvelope{}, parseErr
	}
	if !r.phoneAllowed(peerPhone) {
		return CallEnvelope{}, ErrCallPeerNotAllowed
	}
	r.mu.Lock()
	entry := r.instances[instanceID]
	if entry == nil || entry.caller == nil || entry.wa == nil || !entry.wa.IsConnected() {
		r.mu.Unlock()
		return CallEnvelope{}, errors.New("instance is disconnected")
	}
	if idempotencyKey != "" {
		if oldID := entry.idempotency[idempotencyKey]; oldID != "" {
			if old := r.calls[oldID]; old != nil {
				snapshot := r.snapshotLocked(old, false)
				r.mu.Unlock()
				return snapshot, nil
			}
		}
	}
	if err := r.reservePhoneLocked(entry, instanceID); err != nil {
		r.mu.Unlock()
		return CallEnvelope{}, err
	}
	caller := entry.caller
	r.mu.Unlock()

	options := meowcaller.CallOptions{Video: media == MediaVideo}
	call, err := caller.CallWithOptions(ctx, peer, options)
	if err != nil {
		r.mu.Lock()
		if entry.active == "pending" {
			entry.active = ""
			if r.phoneCalls[entry.accountPhone] == "pending:"+instanceID {
				delete(r.phoneCalls, entry.accountPhone)
			}
			r.active--
		}
		r.mu.Unlock()
		return CallEnvelope{}, err
	}
	return r.addOutgoing(instanceID, entry, call, peer, peerPhone, media, idempotencyKey)
}

func (r *Runtime) reservePhoneLocked(entry *instanceCalls, instanceID string) error {
	if r.drainOwner != "" {
		return ErrCallDrainConflict
	}
	if entry == nil || entry.accountPhone == "" {
		return ErrDisabled
	}
	if entry.active != "" || r.phoneCalls[entry.accountPhone] != "" {
		return ErrCallBusy
	}
	if r.active >= r.options.MaxActive {
		return ErrCallCapacity
	}
	entry.active = "pending"
	r.phoneCalls[entry.accountPhone] = "pending:" + instanceID
	r.active++
	return nil
}

// DeploymentDrain atomically pauses new managed calls and reports privacy-safe
// counts. Existing calls are not changed; the caller must release on refusal.
func (r *Runtime) DeploymentDrain(owner, action string) (DrainStatus, error) {
	if len(owner) < 32 {
		return DrainStatus{}, errors.New("owner must be a random capability of at least 128 bits")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch action {
	case "acquire":
		if r.drainOwner != "" && subtle.ConstantTimeCompare([]byte(r.drainOwner), []byte(owner)) != 1 {
			return DrainStatus{}, ErrCallDrainConflict
		}
		r.drainOwner = owner
	case "release":
		if r.drainOwner != "" && subtle.ConstantTimeCompare([]byte(r.drainOwner), []byte(owner)) != 1 {
			return DrainStatus{}, ErrCallDrainConflict
		}
		r.drainOwner = ""
	default:
		return DrainStatus{}, errors.New("action must be acquire or release")
	}
	status := DrainStatus{Acquired: r.drainOwner != ""}
	if status.Acquired {
		status.Owner = owner
	}
	for _, entry := range r.instances {
		if entry.active == "pending" {
			status.Pending++
		}
	}
	for _, call := range r.calls {
		call.mu.Lock()
		if call.envelope.State != CallStateEnded {
			status.Active++
		}
		call.mu.Unlock()
	}
	return status, nil
}

func (r *Runtime) addOutgoing(instanceID string, reservedEntry *instanceCalls, call *meowcaller.Call, peer, peerPhone, media, key string) (CallEnvelope, error) {
	owner, err := randomID()
	if err != nil {
		_ = call.Hangup()
		return CallEnvelope{}, err
	}
	r.mu.Lock()
	entry := r.instances[instanceID]
	reservation := "pending:" + instanceID
	if entry != reservedEntry || entry == nil || entry.active != "pending" || r.phoneCalls[reservedEntry.accountPhone] != reservation {
		if reservedEntry.active == "pending" {
			reservedEntry.active = ""
			if r.phoneCalls[reservedEntry.accountPhone] == reservation {
				delete(r.phoneCalls, reservedEntry.accountPhone)
			}
			if r.active > 0 {
				r.active--
			}
		}
		r.mu.Unlock()
		_ = call.Hangup()
		return CallEnvelope{}, errors.New("instance ended while dialing")
	}
	id := uuid.NewString()
	now := time.Now().UTC()
	live := &liveCall{envelope: CallEnvelope{CallID: id, InstanceID: instanceID, Peer: call.Peer().String(), Direction: "outbound", Media: media, State: CallStateDialing, Version: 1, CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli()}, call: call, owner: owner, lastBeat: now, accountPhone: reservedEntry.accountPhone, audioIn: make(chan []float32, 12)}
	if peer != "" {
		live.envelope.Peer = peer
	}
	r.calls[id] = live
	entry.active = id
	r.phoneCalls[reservedEntry.accountPhone] = id
	if key != "" {
		entry.idempotency[key] = id
	}
	r.mu.Unlock()
	r.attachCallCallbacks(live)
	r.publish(live)
	env := r.snapshotLocked(live, true)
	return env, nil
}

func (r *Runtime) addIncoming(instanceID string, call *meowcaller.Call) {
	if !r.EnabledFor(instanceID) {
		// Do not claim, answer, or reject calls while browser calling is disabled.
		return
	}
	_, peerPhone, parseErr := r.resolvePeer(instanceID, call.Peer().String())
	if parseErr != nil || !r.phoneAllowed(peerPhone) {
		// Preserve the existing WhatsApp handler behavior for calls outside the
		// gray-release allowlist. Capability gating must not reject them.
		return
	}
	r.mu.Lock()
	entry := r.instances[instanceID]
	if r.drainOwner != "" {
		r.mu.Unlock()
		_ = call.Reject()
		return
	}
	if entry == nil || r.closed || entry.active != "" || r.phoneCalls[entry.accountPhone] != "" || r.active >= r.options.MaxActive {
		r.mu.Unlock()
		_ = call.Reject()
		return
	}
	newID, err := randomID()
	if err != nil {
		r.mu.Unlock()
		_ = call.Reject()
		return
	}
	now := time.Now().UTC()
	media := MediaAudio
	if call.IsVideo() {
		media = MediaVideo
	}
	live := &liveCall{envelope: CallEnvelope{CallID: newID, InstanceID: instanceID, Peer: call.Peer().String(), Direction: "inbound", Media: media, State: CallStateRinging, Version: 1, CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli()}, call: call, accountPhone: entry.accountPhone, audioIn: make(chan []float32, 12)}
	r.calls[newID] = live
	entry.active = newID
	r.phoneCalls[entry.accountPhone] = newID
	r.active++
	r.mu.Unlock()
	r.attachCallCallbacks(live)
	r.publish(live)
}

func (r *Runtime) attachCallCallbacks(call *liveCall) {
	call.call.OnStateChange(func(phase meowcaller.CallPhase) {
		call.mu.Lock()
		switch phase {
		case meowcaller.CallPhaseCalling:
			r.transitionLocked(call, CallStateDialing, "")
		case meowcaller.CallPhaseRinging:
			r.transitionLocked(call, CallStateRinging, "")
		case meowcaller.CallPhaseConnecting:
			r.transitionLocked(call, CallStateConnecting, "")
		case meowcaller.CallPhaseActive:
			call.waReady = true
			r.activateLocked(call)
		case meowcaller.CallPhaseEnded:
			// The library follows this callback synchronously with OnEnd(reason).
			// Publish only the authoritative reason from OnEnd.
		}
		call.mu.Unlock()
		r.publish(call)
	})
	call.call.OnReady(func() {
		call.mu.Lock()
		call.waReady = true
		r.activateLocked(call)
		call.mu.Unlock()
		r.publish(call)
	})
	call.call.OnEnd(func(reason string) {
		call.mu.Lock()
		r.endLocked(call, reason)
		call.mu.Unlock()
		r.release(call)
		r.publish(call)
	})
	if phase := call.call.State(); phase == meowcaller.CallPhaseActive {
		call.mu.Lock()
		call.waReady = true
		r.activateLocked(call)
		call.mu.Unlock()
	}
}

func (r *Runtime) transitionLocked(call *liveCall, state, reason string) {
	if call.envelope.State == CallStateEnded {
		return
	}
	if state == CallStateActive {
		r.activateLocked(call)
		return
	}
	if call.envelope.State == state {
		return
	}
	if callStateRank(state) < callStateRank(call.envelope.State) {
		return
	}
	call.envelope.State = state
	if state == CallStateConnecting && call.peerStartAt.IsZero() {
		call.peerStartAt = time.Now().UTC()
	}
	call.envelope.Version++
	call.envelope.UpdatedAt = time.Now().UTC().UnixMilli()
	if reason != "" {
		call.envelope.EndReason = reason
	}
}

func (r *Runtime) activateLocked(call *liveCall) {
	if call.envelope.State == CallStateEnded {
		return
	}
	if call.waReady && call.peerReady {
		if call.envelope.State != CallStateActive {
			now := time.Now().UTC().UnixMilli()
			call.envelope.State = CallStateActive
			if call.envelope.ConnectedAt == 0 {
				call.envelope.ConnectedAt = now
			}
			call.envelope.Version++
			call.envelope.UpdatedAt = now
		}
		return
	}
	if call.envelope.State != CallStateConnecting {
		call.envelope.State = CallStateConnecting
		if call.peerStartAt.IsZero() {
			call.peerStartAt = time.Now().UTC()
		}
		call.envelope.Version++
		call.envelope.UpdatedAt = time.Now().UTC().UnixMilli()
	}
}

func (r *Runtime) endLocked(call *liveCall, reason string) {
	if call.envelope.State == CallStateEnded {
		return
	}
	now := time.Now().UTC().UnixMilli()
	call.envelope.State = CallStateEnded
	call.envelope.EndReason = reason
	call.envelope.EndedAt = now
	call.envelope.Version++
	call.envelope.UpdatedAt = now
}

func callStateRank(state string) int {
	switch state {
	case CallStateDialing:
		return 1
	case CallStateRinging:
		return 2
	case CallStateConnecting:
		return 3
	case CallStateActive:
		return 4
	case CallStateEnded:
		return 5
	default:
		return 0
	}
}

func (r *Runtime) release(call *liveCall) {
	r.mu.Lock()
	if call.accountPhone != "" && r.phoneCalls[call.accountPhone] == call.envelope.CallID {
		delete(r.phoneCalls, call.accountPhone)
	}
	if entry := r.instances[call.envelope.InstanceID]; entry != nil && entry.active == call.envelope.CallID {
		entry.active = ""
		if r.active > 0 {
			r.active--
		}
	}
	r.mu.Unlock()
	call.closeMedia()
}

func (r *Runtime) failMedia(call *liveCall, reason string) {
	if call == nil {
		return
	}
	call.mu.Lock()
	r.endLocked(call, reason)
	call.mu.Unlock()
	if call.call != nil {
		go call.call.Hangup()
	}
	r.release(call)
	r.publish(call)
}

func (r *Runtime) snapshotLocked(call *liveCall, includeOwner bool) CallEnvelope {
	call.mu.Lock()
	defer call.mu.Unlock()
	snapshot := call.envelope
	if includeOwner {
		snapshot.ControllerID = call.owner
	}
	return snapshot
}

func (r *Runtime) Snapshot(instanceID string) (CallEnvelope, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.instances[instanceID]
	if entry == nil || entry.active == "" || entry.active == "pending" {
		return CallEnvelope{}, false
	}
	call := r.calls[entry.active]
	if call == nil {
		return CallEnvelope{}, false
	}
	return r.snapshotLocked(call, false), true
}

func (r *Runtime) claim(instanceID, callID, requestID string) (*liveCall, string, bool, error) {
	if len(requestID) < 32 {
		return nil, "", false, errors.New("requestId must be a random capability of at least 128 bits")
	}
	r.mu.Lock()
	call := r.calls[callID]
	if call == nil {
		r.mu.Unlock()
		return nil, "", false, ErrNotFound
	}
	call.mu.Lock()
	if call.envelope.InstanceID != instanceID {
		call.mu.Unlock()
		r.mu.Unlock()
		return nil, "", false, ErrNotFound
	}
	if call.envelope.Direction != "inbound" {
		call.mu.Unlock()
		r.mu.Unlock()
		return nil, "", false, ErrCallConflict
	}
	if call.envelope.State == CallStateEnded && (call.owner == "" || subtle.ConstantTimeCompare([]byte(requestID), []byte(call.claimID)) != 1) {
		call.mu.Unlock()
		r.mu.Unlock()
		return nil, "", false, ErrCallEnded
	}
	replay := false
	if call.owner == "" {
		owner, err := randomID()
		if err != nil {
			call.mu.Unlock()
			r.mu.Unlock()
			return nil, "", false, err
		}
		call.owner, call.claimID, call.lastBeat = owner, requestID, time.Now().UTC()
		call.answerDone = make(chan struct{})
	} else if subtle.ConstantTimeCompare([]byte(requestID), []byte(call.claimID)) != 1 {
		call.mu.Unlock()
		r.mu.Unlock()
		return nil, "", false, ErrCallConflict
	} else {
		replay = true
	}
	owner := call.owner
	done := call.answerDone
	call.mu.Unlock()
	r.mu.Unlock()
	if replay && done != nil {
		<-done
	}
	return call, owner, replay, nil
}

func (r *Runtime) Answer(instanceID, callID, requestID string) (CallEnvelope, error) {
	call, owner, replay, err := r.claim(instanceID, callID, requestID)
	if err != nil {
		return CallEnvelope{}, err
	}
	if replay {
		call.mu.Lock()
		env := call.envelope
		call.mu.Unlock()
		return env, nil
	}
	if call.answerFn != nil {
		err = call.answerFn()
	} else if call.call != nil {
		err = call.call.Answer()
	} else {
		err = errors.New("WhatsApp call is unavailable")
	}
	call.mu.Lock()
	if err != nil {
		r.endLocked(call, "answer_failed")
	} else {
		call.envelope.Direction = "inbound"
		r.transitionLocked(call, CallStateConnecting, "")
	}
	done := call.answerDone
	call.answerDone = nil
	if done != nil {
		close(done)
	}
	env := call.envelope
	if err == nil {
		env.ControllerID = owner
	}
	call.mu.Unlock()
	if err != nil {
		r.release(call)
		r.publish(call)
		return CallEnvelope{}, err
	}
	r.publish(call)
	return env, nil
}

func (r *Runtime) Control(instanceID, callID, owner, action string) (CallEnvelope, error) {
	call, ended, err := r.lookupOwnedState(instanceID, callID, owner, true)
	if err != nil {
		return CallEnvelope{}, err
	}
	if ended {
		return r.snapshotLocked(call, false), nil
	}
	if call.call == nil {
		return CallEnvelope{}, ErrCallEnded
	}
	switch action {
	case "reject":
		err = call.call.Reject()
	case "hangup":
		err = call.call.Hangup()
	default:
		return CallEnvelope{}, errors.New("unsupported call action")
	}
	if err != nil {
		call.mu.Lock()
		ended = call.envelope.State == CallStateEnded
		env := call.envelope
		call.mu.Unlock()
		if ended {
			return env, nil
		}
		return CallEnvelope{}, err
	}
	call.mu.Lock()
	r.endLocked(call, action)
	env := call.envelope
	call.mu.Unlock()
	r.release(call)
	r.publish(call)
	return env, nil
}

// RejectUnclaimed allows an authenticated instance caller to decline an
// inbound ring before any browser controller has claimed it. Claiming/answering
// and rejecting serialize under the same locks, so exactly one wins.
func (r *Runtime) RejectUnclaimed(instanceID, callID string) (CallEnvelope, error) {
	r.mu.Lock()
	call := r.calls[callID]
	if call == nil || call.envelope.InstanceID != instanceID {
		r.mu.Unlock()
		return CallEnvelope{}, ErrNotFound
	}
	call.mu.Lock()
	if call.envelope.Direction != "inbound" || call.owner != "" {
		call.mu.Unlock()
		r.mu.Unlock()
		return CallEnvelope{}, ErrCallConflict
	}
	if call.envelope.State == CallStateEnded {
		call.mu.Unlock()
		r.mu.Unlock()
		return CallEnvelope{}, ErrCallEnded
	}
	r.endLocked(call, "rejected")
	env := call.envelope
	call.mu.Unlock()
	r.mu.Unlock()
	if call.call != nil {
		_ = call.call.Reject()
	}
	r.release(call)
	r.publish(call)
	return env, nil
}

// RejectWhatsAppCall rejects a managed inbound call through meowcaller and
// reports false when browser calls are disabled or no matching managed call
// exists yet, allowing the legacy CallOffer handler to keep its old behavior.
func (r *Runtime) RejectWhatsAppCall(instanceID, whatsappCallID string) bool {
	r.mu.Lock()
	entry := r.instances[instanceID]
	if entry == nil || entry.active == "" || entry.active == "pending" {
		r.mu.Unlock()
		return false
	}
	call := r.calls[entry.active]
	if call == nil || call.call == nil || call.call.ID() != whatsappCallID {
		r.mu.Unlock()
		return false
	}
	call.mu.Lock()
	if call.envelope.State == CallStateEnded || call.envelope.Direction != "inbound" || call.owner != "" {
		call.mu.Unlock()
		r.mu.Unlock()
		return false
	}
	r.endLocked(call, "rejected")
	call.mu.Unlock()
	r.mu.Unlock()
	_ = call.call.Reject()
	r.release(call)
	r.publish(call)
	return true
}

func (r *Runtime) Heartbeat(instanceID, callID, owner string) error {
	call, err := r.lookupOwned(instanceID, callID, owner)
	if err != nil {
		return err
	}
	call.mu.Lock()
	call.lastBeat = time.Now().UTC()
	call.mu.Unlock()
	return nil
}

func (r *Runtime) lookupOwned(instanceID, callID, owner string) (*liveCall, error) {
	call, _, err := r.lookupOwnedState(instanceID, callID, owner, false)
	return call, err
}

func (r *Runtime) lookupOwnedState(instanceID, callID, owner string, allowEnded bool) (*liveCall, bool, error) {
	if owner == "" {
		return nil, false, ErrOwner
	}
	r.mu.Lock()
	call := r.calls[callID]
	r.mu.Unlock()
	if call == nil {
		return nil, false, ErrNotFound
	}
	call.mu.Lock()
	if call.envelope.InstanceID != instanceID {
		call.mu.Unlock()
		return nil, false, ErrNotFound
	}
	if call.owner != owner {
		call.mu.Unlock()
		return nil, false, ErrOwner
	}
	ended := call.envelope.State == CallStateEnded
	if ended && !allowEnded {
		call.mu.Unlock()
		return nil, true, ErrCallEnded
	}
	call.mu.Unlock()
	return call, ended, nil
}

func (r *Runtime) checkEnabled(instanceID string) error {
	if !r.EnabledFor(instanceID) {
		return ErrDisabled
	}
	return nil
}

func (r *Runtime) publish(call *liveCall) {
	r.mu.Lock()
	entry := r.instances[call.envelope.InstanceID]
	publisher := r.options.Publish
	r.mu.Unlock()
	if entry == nil || publisher == nil {
		return
	}
	publisher(entry.instance, r.snapshotLocked(call, false))
}

func randomID() (string, error) {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate call controller id: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

func (call *liveCall) closeMedia() {
	call.closeOnce.Do(func() {
		call.mu.Lock()
		pc := call.pc
		call.pc = nil
		call.mu.Unlock()
		if pc != nil {
			_ = pc.Close()
		}
		call.codecMu.Lock()
		codec := call.audioCodec
		call.audioCodec = nil
		if codec != nil {
			_ = codec.Close()
		}
		call.codecMu.Unlock()
	})
}

func (r *Runtime) AddCandidate(instanceID, callID, owner string, candidate webrtc.ICECandidateInit) error {
	call, err := r.lookupOwned(instanceID, callID, owner)
	if err != nil {
		return err
	}
	call.mu.Lock()
	pc := call.pc
	call.mu.Unlock()
	if pc == nil {
		return errors.New("call has no browser media session")
	}
	return pc.AddICECandidate(candidate)
}

func (r *Runtime) CleanupInstance(instanceID, reason string) {
	r.mu.Lock()
	entry := r.instances[instanceID]
	var calls []*liveCall
	if entry != nil {
		calls = r.cleanupInstanceLocked(instanceID, entry, reason)
		delete(r.instances, instanceID)
	}
	r.mu.Unlock()
	for _, call := range calls {
		if call.call != nil {
			go call.call.Hangup()
		}
		call.closeMedia()
		r.publish(call)
	}
}

// EndCalls ends the current session while retaining the meowcaller client so
// whatsmeow's own reconnect loop can continue on the paired companion.
func (r *Runtime) EndCalls(instanceID, reason string) {
	r.mu.Lock()
	entry := r.instances[instanceID]
	var call *liveCall
	if entry != nil && entry.active != "" && entry.active != "pending" {
		call = r.calls[entry.active]
		if call != nil {
			call.mu.Lock()
			r.endLocked(call, reason)
			call.mu.Unlock()
		}
		entry.active = ""
		if call != nil && call.accountPhone != "" && r.phoneCalls[call.accountPhone] == call.envelope.CallID {
			delete(r.phoneCalls, call.accountPhone)
		}
		if r.active > 0 {
			r.active--
		}
	} else if entry != nil && entry.active == "pending" {
		entry.active = ""
		if entry.accountPhone != "" && r.phoneCalls[entry.accountPhone] == "pending:"+instanceID {
			delete(r.phoneCalls, entry.accountPhone)
		}
		if r.active > 0 {
			r.active--
		}
	}
	r.mu.Unlock()
	if call != nil {
		if call.call != nil {
			go call.call.Hangup()
		}
		call.closeMedia()
		r.publish(call)
	}
}

func (r *Runtime) cleanupInstanceLocked(instanceID string, entry *instanceCalls, reason string) []*liveCall {
	var calls []*liveCall
	if entry.active != "" {
		if entry.active == "pending" {
			entry.active = ""
			if entry.accountPhone != "" && r.phoneCalls[entry.accountPhone] == "pending:"+instanceID {
				delete(r.phoneCalls, entry.accountPhone)
			}
			if r.active > 0 {
				r.active--
			}
		} else {
			activeID := entry.active
			if call := r.calls[activeID]; call != nil {
				call.mu.Lock()
				r.endLocked(call, reason)
				call.mu.Unlock()
				calls = append(calls, call)
			}
			entry.active = ""
			if entry.accountPhone != "" && r.phoneCalls[entry.accountPhone] == activeID {
				delete(r.phoneCalls, entry.accountPhone)
			}
			r.active--
			if r.active < 0 {
				r.active = 0
			}
		}
	}
	for key := range entry.idempotency {
		delete(entry.idempotency, key)
	}
	return calls
}

// StartMaintenance bounds orphaned calls after browser loss and unanswered
// ringing/dialing sessions. Stop the returned function during process shutdown.
func (r *Runtime) StartMaintenance(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				r.expireCalls(now.UTC())
			}
		}
	}()
	return cancel
}

func (r *Runtime) expireCalls(now time.Time) {
	type expiredCall struct {
		call   *liveCall
		reason string
	}
	var expired []expiredCall
	r.mu.Lock()
	for _, call := range r.calls {
		call.mu.Lock()
		if call.envelope.State != CallStateEnded {
			reason := ""
			if !call.lastBeat.IsZero() && now.Sub(call.lastBeat) > r.options.HeartbeatTTL {
				reason = "controller_timeout"
			} else if call.envelope.State == CallStateDialing && now.Sub(time.UnixMilli(call.envelope.CreatedAt)) > 45*time.Second {
				reason = "dial_timeout"
			} else if call.envelope.State == CallStateRinging && now.Sub(time.UnixMilli(call.envelope.CreatedAt)) > 60*time.Second {
				reason = "ring_timeout"
			} else if !call.peerLostAt.IsZero() && now.Sub(call.peerLostAt) > 15*time.Second {
				reason = "browser_media_disconnected"
			} else if !call.peerReady && !call.peerStartAt.IsZero() && now.Sub(call.peerStartAt) > 45*time.Second {
				reason = "browser_media_timeout"
			}
			if reason != "" {
				r.endLocked(call, reason)
				expired = append(expired, expiredCall{call: call, reason: reason})
			}
		}
		call.mu.Unlock()
	}
	for id, call := range r.calls {
		call.mu.Lock()
		old := call.envelope.State == CallStateEnded && now.Sub(time.UnixMilli(call.envelope.UpdatedAt)) > time.Hour
		call.mu.Unlock()
		if old {
			delete(r.calls, id)
			for _, entry := range r.instances {
				for key, callID := range entry.idempotency {
					if callID == id {
						delete(entry.idempotency, key)
					}
				}
			}
		}
	}
	r.mu.Unlock()
	for _, item := range expired {
		if item.call.call != nil {
			_ = item.call.call.Hangup()
		}
		r.release(item.call)
		r.publish(item.call)
	}
}

func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.drainOwner = ""
	var calls []*liveCall
	for id, entry := range r.instances {
		calls = append(calls, r.cleanupInstanceLocked(id, entry, "engine_shutdown")...)
	}
	conn, mux := r.udpConn, r.mux
	r.instances = make(map[string]*instanceCalls)
	r.phoneCalls = make(map[string]string)
	r.mu.Unlock()
	for _, call := range calls {
		if call.call != nil {
			go call.call.Hangup()
		}
		call.closeMedia()
	}
	if mux != nil {
		mux.Close()
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}
