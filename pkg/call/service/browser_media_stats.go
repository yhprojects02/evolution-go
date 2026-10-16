package call_service

import (
	"math"
)

func sanitizeBrowserMediaStats(input *BrowserMediaStats) *BrowserMediaStats {
	if input == nil {
		return nil
	}
	result := &BrowserMediaStats{}
	if transport := input.Transport; transport != nil {
		clean := &BrowserTransportStats{}
		if oneOf(transport.LocalCandidateType, "host", "srflx", "prflx", "relay") {
			clean.LocalCandidateType = transport.LocalCandidateType
		}
		if oneOf(transport.LocalProtocol, "udp", "tcp") {
			clean.LocalProtocol = transport.LocalProtocol
		}
		if oneOf(transport.RelayProtocol, "udp", "tcp", "tls") {
			clean.RelayProtocol = transport.RelayProtocol
		}
		clean.CurrentRoundTripMs = boundedFloatPointer(transport.CurrentRoundTripMs, 0, 120_000)
		if clean.LocalCandidateType != "" || clean.LocalProtocol != "" || clean.RelayProtocol != "" || clean.CurrentRoundTripMs != nil {
			result.Transport = clean
		}
	}
	if audio := input.InboundAudio; audio != nil {
		clean := &BrowserInboundAudioStats{}
		clean.AverageJitterBufferMs = boundedFloatPointer(audio.AverageJitterBufferMs, 0, 60_000)
		clean.JitterMs = boundedFloatPointer(audio.JitterMs, 0, 60_000)
		if audio.PacketsLost != nil && *audio.PacketsLost >= -1_000_000_000 && *audio.PacketsLost <= 1_000_000_000 {
			value := *audio.PacketsLost
			clean.PacketsLost = &value
		}
		if audio.PacketsReceived != nil && *audio.PacketsReceived <= 1_000_000_000_000 {
			value := *audio.PacketsReceived
			clean.PacketsReceived = &value
		}
		if clean.AverageJitterBufferMs != nil || clean.JitterMs != nil || clean.PacketsLost != nil || clean.PacketsReceived != nil {
			result.InboundAudio = clean
		}
	}
	if result.Transport == nil && result.InboundAudio == nil {
		return nil
	}
	return result
}

func boundedFloatPointer(value *float64, min, max float64) *float64 {
	if value == nil || math.IsNaN(*value) || math.IsInf(*value, 0) || *value < min || *value > max {
		return nil
	}
	copy := *value
	return &copy
}

func oneOf(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}
