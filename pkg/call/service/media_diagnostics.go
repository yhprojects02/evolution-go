package call_service

import (
	"math"
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/purpshell/meowcaller"
)

// mediaQueueDiagnostics records bounded, content-free queue measurements.
type mediaQueueDiagnostics struct {
	mu               sync.Mutex
	depth            int
	capacity         int
	maxDepth         int
	maxDequeuedAgeMs int64
	overflowDrops    uint64
}

type queuedAudioFrame struct {
	samples    []float32
	enqueuedAt time.Time
}

type queuedRTPPacket struct {
	packet     *rtp.Packet
	enqueuedAt time.Time
}

type QueueDiagnostics struct {
	Depth            int    `json:"depth"`
	Capacity         int    `json:"capacity"`
	MaxDepth         int    `json:"maxDepth"`
	MaxDequeuedAgeMs int64  `json:"maxDequeuedAgeMs"`
	OverflowDrops    uint64 `json:"overflowDrops"`
}

func (q *mediaQueueDiagnostics) pushLocked(depth int, dropped bool) {
	q.depth = depth
	if depth > q.maxDepth {
		q.maxDepth = depth
	}
	if dropped {
		q.overflowDrops++
	}
}

func (q *mediaQueueDiagnostics) popLocked(depth int, enqueuedAt time.Time) {
	q.depth = depth
	if !enqueuedAt.IsZero() {
		if age := time.Since(enqueuedAt).Milliseconds(); age > q.maxDequeuedAgeMs {
			q.maxDequeuedAgeMs = age
		}
	}
}

func (q *mediaQueueDiagnostics) snapshot() QueueDiagnostics {
	q.mu.Lock()
	defer q.mu.Unlock()
	return QueueDiagnostics{Depth: q.depth, Capacity: q.capacity, MaxDepth: q.maxDepth,
		MaxDequeuedAgeMs: q.maxDequeuedAgeMs, OverflowDrops: q.overflowDrops}
}

type TrackProcessingDiagnostics struct {
	SinkCallbacks uint64  `json:"sinkCallbacks"`
	ProcessMeanMs float64 `json:"processMeanMs"`
	ProcessMaxMs  float64 `json:"processMaxMs"`
	WriteMeanMs   float64 `json:"writeMeanMs"`
	WriteMaxMs    float64 `json:"writeMaxMs"`
}

type PionPathDiagnostics struct {
	Available           bool     `json:"available"`
	CurrentRoundTripMs  *float64 `json:"currentRoundTripMs,omitempty"`
	LocalCandidateType  string   `json:"localCandidateType,omitempty"`
	RemoteCandidateType string   `json:"remoteCandidateType,omitempty"`
	LocalProtocol       string   `json:"localProtocol,omitempty"`
	RemoteProtocol      string   `json:"remoteProtocol,omitempty"`
}

type CallMediaDiagnostics struct {
	BrowserRTPQueue       QueueDiagnostics                      `json:"browserRtpQueue"`
	BrowserPCMQueue       QueueDiagnostics                      `json:"browserPcmQueue"`
	WhatsAppToBrowserSink TrackProcessingDiagnostics            `json:"whatsappToBrowserSink"`
	PionPath              PionPathDiagnostics                   `json:"pionPath"`
	WhatsAppRelay         *meowcaller.MediaTransportDiagnostics `json:"whatsappRelay,omitempty"`
	Browser               *BrowserMediaStats                    `json:"browser,omitempty"`
}

type callMediaDiagnostics struct {
	browserRTPQueue mediaQueueDiagnostics
	browserPCMQueue mediaQueueDiagnostics
	mu              sync.Mutex
	sinkCallbacks   uint64
	processTotalMs  float64
	processMaxMs    float64
	writeTotalMs    float64
	writeMaxMs      float64
}

func newCallMediaDiagnostics() *callMediaDiagnostics {
	return &callMediaDiagnostics{
		browserRTPQueue: mediaQueueDiagnostics{capacity: 32},
		browserPCMQueue: mediaQueueDiagnostics{capacity: 12},
	}
}

func (d *callMediaDiagnostics) recordSink(process, write time.Duration) {
	if d == nil {
		return
	}
	processMs, writeMs := float64(process.Microseconds())/1000, float64(write.Microseconds())/1000
	d.mu.Lock()
	d.sinkCallbacks++
	d.processTotalMs += processMs
	d.writeTotalMs += writeMs
	d.processMaxMs = max(d.processMaxMs, processMs)
	d.writeMaxMs = max(d.writeMaxMs, writeMs)
	d.mu.Unlock()
}

func (d *callMediaDiagnostics) snapshot(pc *webrtc.PeerConnection, call *meowcaller.Call) CallMediaDiagnostics {
	result := CallMediaDiagnostics{
		BrowserRTPQueue: d.browserRTPQueue.snapshot(),
		BrowserPCMQueue: d.browserPCMQueue.snapshot(),
		PionPath:        pionPathDiagnostics(pc),
	}
	d.mu.Lock()
	result.WhatsAppToBrowserSink.SinkCallbacks = d.sinkCallbacks
	if d.sinkCallbacks > 0 {
		result.WhatsAppToBrowserSink.ProcessMeanMs = d.processTotalMs / float64(d.sinkCallbacks)
		result.WhatsAppToBrowserSink.WriteMeanMs = d.writeTotalMs / float64(d.sinkCallbacks)
	}
	result.WhatsAppToBrowserSink.ProcessMaxMs = d.processMaxMs
	result.WhatsAppToBrowserSink.WriteMaxMs = d.writeMaxMs
	d.mu.Unlock()
	if call != nil {
		if relay, ok := call.CurrentMediaTransportDiagnostics(); ok {
			result.WhatsAppRelay = &relay
		}
	}
	return result
}

func pionPathDiagnostics(pc *webrtc.PeerConnection) PionPathDiagnostics {
	if pc == nil || pc.SCTP() == nil || pc.SCTP().Transport() == nil || pc.SCTP().Transport().ICETransport() == nil {
		return PionPathDiagnostics{}
	}
	iceTransport := pc.SCTP().Transport().ICETransport()
	pair, err := iceTransport.GetSelectedCandidatePair()
	if err != nil || pair == nil {
		return PionPathDiagnostics{}
	}
	stats, available := iceTransport.GetSelectedCandidatePairStats()
	return pionPathDiagnosticsFromPair(pair, stats, available)
}

func pionPathDiagnosticsFromPair(pair *webrtc.ICECandidatePair, stats webrtc.ICECandidatePairStats, statsAvailable bool) PionPathDiagnostics {
	result := PionPathDiagnostics{}
	if pair == nil || pair.Local == nil || pair.Remote == nil {
		return result
	}
	result.Available = true
	if statsAvailable && stats.ResponsesReceived > 0 && stats.CurrentRoundTripTime > 0 && !math.IsNaN(stats.CurrentRoundTripTime) && !math.IsInf(stats.CurrentRoundTripTime, 0) {
		rtt := stats.CurrentRoundTripTime * 1000 // Pion exposes seconds.
		result.CurrentRoundTripMs = &rtt
	}
	result.LocalCandidateType = safeCandidateType(pair.Local.Typ)
	result.LocalProtocol = safeCandidateProtocol(pair.Local.Protocol.String())
	result.RemoteCandidateType = safeCandidateType(pair.Remote.Typ)
	result.RemoteProtocol = safeCandidateProtocol(pair.Remote.Protocol.String())
	return result
}

func safeCandidateProtocol(protocol string) string {
	if protocol == "udp" || protocol == "tcp" {
		return protocol
	}
	return ""
}

func safeCandidateType(candidateType webrtc.ICECandidateType) string {
	switch candidateType {
	case webrtc.ICECandidateTypeHost, webrtc.ICECandidateTypeSrflx, webrtc.ICECandidateTypePrflx, webrtc.ICECandidateTypeRelay:
		return candidateType.String()
	default:
		return ""
	}
}
