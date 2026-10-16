package call_service

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
	"github.com/purpshell/meowcaller"
)

const (
	callAudioRate           = 16000
	callAudioFrame          = 960
	callVideoFPS            = 24
	maxVideoAUBytes         = 2 * 1024 * 1024
	maxAudioReorderPackets  = 8
	audioReorderDelay       = 40 * time.Millisecond
	maxAudioPLC48Samples    = 5760
	callAudioSampleDuration = 60 * time.Millisecond
)

// Offer accepts a browser SDP offer and returns the fully gathered server answer.
func (r *Runtime) Offer(instanceID, callID, owner, sdp string) (string, error) {
	if strings.TrimSpace(sdp) == "" {
		return "", errors.New("sdp offer is required")
	}
	call, err := r.lookupOwned(instanceID, callID, owner)
	if err != nil {
		return "", err
	}
	call.mu.Lock()
	if call.pcOfferPending {
		call.mu.Unlock()
		return "", errors.New("browser offer is already being processed")
	}
	call.pcOfferPending = true
	call.peerStartAt = time.Now().UTC()
	call.peerLostAt = time.Time{}
	call.peerReady = false
	previous := call.pc
	call.pc = nil
	r.activateLocked(call)
	call.mu.Unlock()
	r.publish(call)
	if previous != nil {
		_ = previous.Close()
	}
	defer func() { call.mu.Lock(); call.pcOfferPending = false; call.mu.Unlock() }()
	pc, err := r.newPeerConnection(instanceID)
	if err != nil {
		r.failMedia(call, "media_failed")
		return "", err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = pc.Close()
		}
	}()
	if err = r.addLocalTracks(call, pc); err != nil {
		r.failMedia(call, "media_failed")
		return "", err
	}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		call.mu.Lock()
		if call.pc != pc || call.envelope.State == CallStateEnded {
			call.mu.Unlock()
			return
		}
		call.peerReady = state == webrtc.PeerConnectionStateConnected
		if state == webrtc.PeerConnectionStateConnected {
			call.peerLostAt = time.Time{}
		}
		if state == webrtc.PeerConnectionStateDisconnected {
			call.peerLostAt = time.Now().UTC()
		}
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			r.endLocked(call, "browser_media_disconnected")
		}
		r.activateLocked(call)
		call.mu.Unlock()
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			r.release(call)
		}
		r.publish(call)
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		go r.readBrowserTrack(call, track, receiver)
	})
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
		r.failMedia(call, "media_failed")
		return "", fmt.Errorf("set browser offer: %w", err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		r.failMedia(call, "media_failed")
		return "", fmt.Errorf("create browser answer: %w", err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(answer); err != nil {
		r.failMedia(call, "media_failed")
		return "", fmt.Errorf("set browser answer: %w", err)
	}
	select {
	case <-gathered:
	case <-time.After(15 * time.Second):
		r.failMedia(call, "media_failed")
		return "", errors.New("ICE candidate gathering timed out")
	}
	call.mu.Lock()
	if call.owner != owner || call.envelope.State == CallStateEnded {
		call.mu.Unlock()
		return "", ErrOwner
	}
	call.pc = pc
	call.envelope.UpdatedAt = time.Now().UTC().UnixMilli()
	if pc.ConnectionState() == webrtc.PeerConnectionStateConnected {
		call.peerReady = true
		call.peerLostAt = time.Time{}
		r.activateLocked(call)
	}
	call.mu.Unlock()
	cleanup = false
	return pc.LocalDescription().SDP, nil
}

func (r *Runtime) newPeerConnection(instanceID string) (*webrtc.PeerConnection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("call runtime is closed")
	}
	if r.mux == nil {
		addr := &net.UDPAddr{IP: net.ParseIP(r.options.UDPBind), Port: r.options.UDPPort}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen for WebRTC media: %w", err)
		}
		r.udpConn = conn
		r.mux = ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: conn})
	}
	settings := webrtc.SettingEngine{}
	settings.SetICEUDPMux(r.mux)
	if r.options.PublicIP != "" {
		settings.SetNAT1To1IPs([]string{r.options.PublicIP}, webrtc.ICECandidateTypeHost)
	}
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("register WebRTC codecs: %w", err)
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptors); err != nil {
		return nil, fmt.Errorf("register WebRTC interceptors: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine), webrtc.WithInterceptorRegistry(interceptors), webrtc.WithSettingEngine(settings))
	configuration := webrtc.Configuration{}
	if urls := turnURLs(r.options.TURNURL); len(urls) > 0 && r.options.TURNSecret != "" {
		username, credential, _, err := r.turnCredentials(instanceID)
		if err != nil {
			return nil, err
		}
		configuration.ICEServers = []webrtc.ICEServer{{URLs: urls, Username: username, Credential: credential}}
	}
	return api.NewPeerConnection(configuration)
}

func (r *Runtime) addLocalTracks(call *liveCall, pc *webrtc.PeerConnection) error {
	audio, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "whatsapp")
	if err != nil {
		return err
	}
	audioSender, err := pc.AddTrack(audio)
	if err != nil {
		return err
	}
	go r.drainSenderRTCP(call, audioSender, false)
	call.mu.Lock()
	call.audioTrack = audio
	attachAudio := !call.mediaAttached
	call.mediaAttached = true
	call.mu.Unlock()
	encoder, err := newCallAudioEncoder()
	if err != nil {
		return fmt.Errorf("initialize browser Opus encoder: %w", err)
	}
	call.codecMu.Lock()
	if call.audioCodec != nil {
		_ = call.audioCodec.Close()
	}
	call.audioCodec = encoder
	call.codecMu.Unlock()
	if attachAudio {
		outbound := &audioPCMAccumulator{}
		call.call.Receive(meowcaller.SinkFunc(func(pcm []float32) {
			processStarted := time.Now()
			var encodedPackets [][]byte
			call.codecMu.Lock()
			if call.audioCodec == nil {
				call.codecMu.Unlock()
				return
			}
			frames := outbound.Push(pcm)
			var encErr error
			for _, frame := range frames {
				encoded := make([]byte, 4000)
				n, frameErr := call.audioCodec.Encode(monoToStereo(frame), encoded)
				if frameErr != nil {
					encErr = frameErr
					break
				}
				if n > 0 {
					encodedPackets = append(encodedPackets, append([]byte(nil), encoded[:n]...))
				}
			}
			call.codecMu.Unlock()
			if encErr != nil {
				r.failMedia(call, "media_failed")
				return
			}
			if len(encodedPackets) == 0 {
				call.mediaDiagnostics.recordSink(time.Since(processStarted), 0)
				return
			}
			processDuration := time.Since(processStarted)
			call.mu.Lock()
			currentAudio := call.audioTrack
			call.mu.Unlock()
			var writeDuration time.Duration
			if currentAudio != nil {
				writeStarted := time.Now()
				for _, encoded := range encodedPackets {
					if writeErr := currentAudio.WriteSample(media.Sample{Data: encoded, Duration: callAudioSampleDuration}); writeErr != nil {
						r.failMedia(call, "media_failed")
						return
					}
				}
				writeDuration = time.Since(writeStarted)
			}
			call.mediaDiagnostics.recordSink(processDuration, writeDuration)
		}))
		call.call.Play(&browserAudioSource{frames: call.audioIn, stats: &call.mediaDiagnostics.browserPCMQueue})
	}
	if call.envelope.Media == MediaVideo {
		video, trackErr := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"}, "video", "whatsapp")
		if trackErr != nil {
			return trackErr
		}
		videoSender, addErr := pc.AddTrack(video)
		if addErr != nil {
			return addErr
		}
		go r.drainSenderRTCP(call, videoSender, true)
		call.mu.Lock()
		call.videoTrack = video
		call.mu.Unlock()
		call.call.ReceiveVideo(browserVideoSink{runtime: r, call: call})
		call.call.OnVideoKeyframeRequest(func() {
			call.mu.Lock()
			ssrc := call.videoSSRC
			call.mu.Unlock()
			if ssrc == 0 {
				return
			}
			if err := pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}}); err != nil {
				return
			}
		})
	}
	return nil
}

type audioPCMAccumulator struct{ pending []float32 }

func (a *audioPCMAccumulator) Push(samples []float32) [][]float32 {
	frames, remaining := splitAudioFrames(a.pending, samples, callAudioFrame)
	a.pending = remaining
	return frames
}

type browserAudioSource struct {
	frames <-chan queuedAudioFrame
	stats  *mediaQueueDiagnostics
}

func (s *browserAudioSource) ReadFrame() ([]float32, error) {
	frame := make([]float32, callAudioFrame)
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	select {
	case item, ok := <-s.frames:
		if !ok {
			return nil, errors.New("browser audio source closed")
		}
		copy(frame, item.samples)
		s.stats.popLocked(len(s.frames), item.enqueuedAt)
	default:
	}
	return frame, nil
}

func (s *browserAudioSource) Close() error { return nil }

func (r *Runtime) readBrowserTrack(call *liveCall, track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	switch track.Codec().MimeType {
	case webrtc.MimeTypeOpus:
		r.readBrowserAudio(call, track)
	case webrtc.MimeTypeH264:
		call.mu.Lock()
		call.videoSSRC = uint32(track.SSRC())
		call.mu.Unlock()
		r.readBrowserVideo(call, track)
	}
}

func (r *Runtime) readBrowserAudio(call *liveCall, track *webrtc.TrackRemote) {
	decoder, err := newCallAudioDecoder()
	if err != nil {
		r.failMedia(call, "media_failed")
		return
	}
	defer decoder.Close()
	reorder := newOpusRTPReorder(maxAudioReorderPackets)
	packetCh := make(chan queuedRTPPacket, 32)
	errCh := make(chan error, 1)
	go func() {
		for {
			packet, _, readErr := track.ReadRTP()
			if readErr != nil {
				errCh <- readErr
				return
			}
			queued := queuedRTPPacket{packet: packet, enqueuedAt: time.Now()}
			stats := &call.mediaDiagnostics.browserRTPQueue
			stats.mu.Lock()
			select {
			case packetCh <- queued:
				stats.pushLocked(len(packetCh), false)
			default:
				stats.pushLocked(len(packetCh), true)
				// A full local queue is treated as packet loss. The sequence gap
				// will be concealed by the ordered decoder below.
			}
			stats.mu.Unlock()
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	ingress := &browserOpusIngress{decoder: decoder, pending: make([]float32, 0, callAudioFrame*2)}
	process := func(ordered, following *rtp.Packet) bool {
		frames, processErr := ingress.Process(ordered, following)
		if processErr != nil {
			r.failMedia(call, "media_failed")
			return false
		}
		queueAudioFrames(call, frames)
		return true
	}
	for {
		select {
		case queued := <-packetCh:
			stats := &call.mediaDiagnostics.browserRTPQueue
			stats.mu.Lock()
			stats.popLocked(len(packetCh), queued.enqueuedAt)
			stats.mu.Unlock()
			if !processReady(reorder.Push(queued.packet, time.Now()), process) {
				return
			}
		case <-ticker.C:
			if !processReady(reorder.Flush(time.Now()), process) {
				return
			}
		case <-errCh:
			return
		}
	}
}

type browserOpusIngress struct {
	decoder              callAudioDecoder
	pending              []float32
	expectedTimestamp    uint32
	hasExpectedTimestamp bool
}

func (ingress *browserOpusIngress) Process(packet, following *rtp.Packet) ([][]float32, error) {
	if packet == nil {
		plcSamples := callAudioFrame // 20 ms at the Opus 48 kHz RTP clock.
		if following != nil && ingress.hasExpectedTimestamp {
			gap := int32(following.Timestamp - ingress.expectedTimestamp)
			if gap <= 0 {
				return nil, nil
			}
			plcSamples = min(int(gap), maxAudioPLC48Samples)
		}
		concealed, samples, err := decodeBrowserOpusPLC(ingress.decoder, plcSamples)
		if err != nil {
			return nil, err
		}
		if samples > 0 {
			ingress.pending = append(ingress.pending, concealed...)
			if ingress.hasExpectedTimestamp {
				ingress.expectedTimestamp += uint32(samples)
			}
		}
		return ingress.popFrames(), nil
	}
	if ingress.hasExpectedTimestamp {
		gap := int32(packet.Timestamp - ingress.expectedTimestamp)
		if gap > 0 {
			concealed, samples, err := decodeBrowserOpusPLC(ingress.decoder, min(int(gap), maxAudioPLC48Samples))
			if err != nil {
				return nil, err
			}
			ingress.pending = append(ingress.pending, concealed...)
			ingress.expectedTimestamp += uint32(samples)
		}
	}
	decoded, samples, err := decodeBrowserOpus(ingress.decoder, packet.Payload)
	if err != nil {
		return nil, err
	}
	if samples > 0 {
		ingress.pending = append(ingress.pending, decoded...)
		ingress.expectedTimestamp = packet.Timestamp + uint32(samples)
		ingress.hasExpectedTimestamp = true
	}
	return ingress.popFrames(), nil
}

func (ingress *browserOpusIngress) popFrames() [][]float32 {
	frames, remaining := splitAudioFrames(ingress.pending, nil, callAudioFrame)
	ingress.pending = remaining
	return frames
}

func queueAudioFrames(call *liveCall, frames [][]float32) {
	stats := &call.mediaDiagnostics.browserPCMQueue
	for _, frame := range frames {
		queued := queuedAudioFrame{samples: frame, enqueuedAt: time.Now()}
		stats.mu.Lock()
		select {
		case call.audioIn <- queued:
			stats.pushLocked(len(call.audioIn), false)
		default:
			select {
			case <-call.audioIn:
				stats.pushLocked(len(call.audioIn), true)
			default:
			}
			select {
			case call.audioIn <- queued:
				stats.pushLocked(len(call.audioIn), false)
			default:
				stats.pushLocked(len(call.audioIn), true)
			}
		}
		stats.mu.Unlock()
	}
}

func processReady(ready []*rtp.Packet, process func(*rtp.Packet, *rtp.Packet) bool) bool {
	for i, packet := range ready {
		var following *rtp.Packet
		for next := i + 1; next < len(ready); next++ {
			if ready[next] != nil {
				following = ready[next]
				break
			}
		}
		if !process(packet, following) {
			return false
		}
	}
	return true
}

func decodeBrowserOpusPLC(decoder callAudioDecoder, requestedSamples int) ([]float32, int, error) {
	if requestedSamples <= 0 {
		return nil, 0, nil
	}
	requestedSamples = min(max(requestedSamples, 120), maxAudioPLC48Samples)
	requestedSamples = requestedSamples / 120 * 120 // Opus frame sizes are 2.5 ms multiples at 48 kHz.
	pcm := make([]float32, requestedSamples*2)
	plcDecoder, ok := decoder.(interface {
		DecodePLC(int, []float32) (int, error)
	})
	if !ok {
		// The non-CGO implementation reports unsupported above; this fallback
		// keeps a decoder without PLC from replaying late packets out of order.
		return make([]float32, requestedSamples/3), requestedSamples, nil
	}
	n, err := plcDecoder.DecodePLC(requestedSamples, pcm)
	if err != nil || n <= 0 {
		return nil, n, err
	}
	mono48 := stereoToMono(pcm[:n*2])
	return resampleMono(mono48, 48000, callAudioRate), n, nil
}

func decodeBrowserOpus(decoder callAudioDecoder, packet []byte) ([]float32, int, error) {
	decoded := make([]float32, 11520)
	n, err := decoder.Decode(packet, decoded)
	if err != nil || n <= 0 {
		return nil, n, err
	}
	mono48 := stereoToMono(decoded[:n*2])
	return resampleMono(mono48, 48000, callAudioRate), n, nil
}

// opusRTPReorder holds a small sequence window. It releases contiguous RTP
// immediately and emits nil for a missing sequence only after the bounded wait.
// The decoder uses nil as PLC and packet timestamps align any variable duration.
type opusRTPReorder struct {
	started   bool
	next      uint16
	maxQueued int
	gapSince  time.Time
	queued    map[uint16]*rtp.Packet
}

func newOpusRTPReorder(maxQueued int) *opusRTPReorder {
	if maxQueued < 1 {
		maxQueued = 1
	}
	return &opusRTPReorder{maxQueued: maxQueued, queued: make(map[uint16]*rtp.Packet, maxQueued)}
}

func (b *opusRTPReorder) Push(packet *rtp.Packet, now time.Time) []*rtp.Packet {
	if packet == nil {
		return nil
	}
	if !b.started {
		b.started = true
		b.next = packet.SequenceNumber
	}
	if int16(packet.SequenceNumber-b.next) < 0 {
		return nil // Duplicate or too late; never rewind Opus decoder state.
	}
	if _, exists := b.queued[packet.SequenceNumber]; exists {
		return nil
	}
	if len(b.queued) >= b.maxQueued {
		// Preserve the nearest forward packets if the consumer is stalled.
		if int16(packet.SequenceNumber-b.next) >= int16(b.maxQueued) {
			return b.release(now, true)
		}
	}
	b.queued[packet.SequenceNumber] = packet
	return b.release(now, false)
}

func (b *opusRTPReorder) Flush(now time.Time) []*rtp.Packet {
	return b.release(now, false)
}

func (b *opusRTPReorder) release(now time.Time, force bool) []*rtp.Packet {
	var ready []*rtp.Packet
	for b.started && len(b.queued) > 0 {
		if packet := b.queued[b.next]; packet != nil {
			delete(b.queued, b.next)
			ready = append(ready, packet)
			b.next++
			b.gapSince = time.Time{}
			continue
		}
		if b.gapSince.IsZero() {
			b.gapSince = now
		}
		if !force && now.Sub(b.gapSince) < audioReorderDelay && len(b.queued) < b.maxQueued {
			break
		}
		ready = append(ready, nil)
		b.next++
		b.gapSince = now
		force = false
	}
	return ready
}

func splitAudioFrames(pending, samples []float32, frameSize int) ([][]float32, []float32) {
	if frameSize <= 0 {
		return nil, pending
	}
	pending = append(pending, samples...)
	frameCount := len(pending) / frameSize
	frames := make([][]float32, 0, frameCount)
	for i := 0; i < frameCount; i++ {
		start := i * frameSize
		frames = append(frames, append([]float32(nil), pending[start:start+frameSize]...))
	}
	remaining := append([]float32(nil), pending[frameCount*frameSize:]...)
	return frames, remaining
}

func stereoToMono(input []float32) []float32 {
	output := make([]float32, len(input)/2)
	for i := range output {
		output[i] = (input[i*2] + input[i*2+1]) / 2
	}
	return output
}

func monoToStereo(input []float32) []float32 {
	output := make([]float32, len(input)*2)
	for i, sample := range input {
		output[i*2], output[i*2+1] = sample, sample
	}
	return output
}

func resampleMono(input []float32, sourceRate, targetRate int) []float32 {
	if sourceRate == targetRate || len(input) == 0 {
		return append([]float32(nil), input...)
	}
	length := len(input) * targetRate / sourceRate
	output := make([]float32, length)
	for i := range output {
		position := float64(i) * float64(sourceRate) / float64(targetRate)
		left := int(position)
		right := left + 1
		if right >= len(input) {
			right = len(input) - 1
		}
		fraction := float32(position - float64(left))
		output[i] = input[left]*(1-fraction) + input[right]*fraction
	}
	return output
}

func (r *Runtime) readBrowserVideo(call *liveCall, track *webrtc.TrackRemote) {
	builder := samplebuilder.New(4, &codecs.H264Packet{}, 90000, samplebuilder.WithMaxTimeDelay(250*time.Millisecond))
	var previousTimestamp uint32
	hasPreviousTimestamp := false
	for {
		packet, _, err := track.ReadRTP()
		if err != nil {
			return
		}
		builder.Push(packet)
		for sample := builder.Pop(); sample != nil; sample = builder.Pop() {
			if len(sample.Data) == 0 || len(sample.Data) > maxVideoAUBytes {
				continue
			}
			duration := time.Second / callVideoFPS
			if hasPreviousTimestamp {
				duration = videoFrameDuration(sample.PacketTimestamp, previousTimestamp, 90000, duration)
			}
			if sendErr := call.call.SendVideoWithDuration(sample.Data, duration); sendErr != nil {
				r.failMedia(call, "media_failed")
				return
			}
			previousTimestamp = sample.PacketTimestamp
			hasPreviousTimestamp = true
		}
	}
}

type browserVideoSink struct {
	runtime *Runtime
	call    *liveCall
}

func (browserVideoSink) Close() error { return nil }

func (s browserVideoSink) SetOrientation(orientation int) {
	if s.runtime == nil || s.call == nil || orientation < 0 || orientation > 3 {
		return
	}
	s.call.mu.Lock()
	if s.call.envelope.VideoOrientation == nil || *s.call.envelope.VideoOrientation != orientation {
		value := orientation
		s.call.envelope.VideoOrientation = &value
		s.call.envelope.Version++
		s.call.envelope.UpdatedAt = time.Now().UTC().UnixMilli()
	}
	s.call.mu.Unlock()
	s.runtime.publish(s.call)
}

func (s browserVideoSink) WriteVideo(accessUnit []byte) error {
	return s.WriteVideoTimestamped(accessUnit, 0, 0)
}

func (s browserVideoSink) WriteVideoTimestamped(accessUnit []byte, timestamp, clockRate uint32) error {
	if len(accessUnit) == 0 || len(accessUnit) > maxVideoAUBytes || s.call == nil {
		return nil
	}
	s.call.mu.Lock()
	track := s.call.videoTrack
	duration := time.Second / callVideoFPS
	if clockRate > 0 && s.call.hasVideoTimestamp {
		duration = videoFrameDuration(timestamp, s.call.videoTimestamp, clockRate, duration)
	}
	if clockRate > 0 {
		s.call.videoTimestamp = timestamp
		s.call.hasVideoTimestamp = true
	}
	s.call.mu.Unlock()
	if track == nil {
		return nil
	}
	return track.WriteSample(media.Sample{Data: append([]byte(nil), accessUnit...), Duration: duration})
}

func videoFrameDuration(current, previous, clockRate uint32, fallback time.Duration) time.Duration {
	if clockRate == 0 || current == previous {
		return fallback
	}
	delta := current - previous
	duration := time.Duration(uint64(delta) * uint64(time.Second) / uint64(clockRate))
	if duration < 5*time.Millisecond || duration > 250*time.Millisecond {
		return fallback
	}
	return duration
}

func (r *Runtime) handlePeerState(call *liveCall, connected bool) {
	call.mu.Lock()
	call.peerReady = connected
	r.activateLocked(call)
	call.mu.Unlock()
	r.publish(call)
}

func (r *Runtime) drainSenderRTCP(call *liveCall, sender *webrtc.RTPSender, video bool) {
	if sender == nil {
		return
	}
	buf := make([]byte, 1500)
	for {
		n, _, err := sender.Read(buf)
		if err != nil {
			return
		}
		packets, err := rtcp.Unmarshal(buf[:n])
		if err != nil {
			continue
		}
		if !video {
			continue
		}
		for _, packet := range packets {
			if _, ok := packet.(*rtcp.PictureLossIndication); ok {
				call.mu.Lock()
				request := call.requestPeerKeyframe
				call.mu.Unlock()
				if request != nil {
					if err := request(); err != nil {
						continue
					}
				} else if call.call != nil {
					if err := call.call.RequestPeerVideoKeyframe(); err != nil {
						// Feedback can arrive before WhatsApp negotiates a video SSRC.
						continue
					}
				}
			}
		}
	}
}
