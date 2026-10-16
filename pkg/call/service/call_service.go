package call_service

import (
	"context"
	"errors"
	"strings"
	"time"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/EvolutionAPI/evolution-go/pkg/logger"
	"github.com/EvolutionAPI/evolution-go/pkg/registry"
	"github.com/gomessguii/logger"
	"github.com/pion/webrtc/v4"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type CallService interface {
	Dial(data *DialCallStruct, instance *instance_model.Instance) (CallEnvelope, error)
	Answer(data *AnswerCallStruct, instance *instance_model.Instance) (CallEnvelope, error)
	RejectCall(data *RejectCallStruct, instance *instance_model.Instance) (CallEnvelope, error)
	Hangup(data *ControlCallStruct, instance *instance_model.Instance) (CallEnvelope, error)
	Current(instance *instance_model.Instance) (CallEnvelope, bool)
	Offer(data *OfferCallStruct, instance *instance_model.Instance) (string, error)
	Candidate(data *CandidateCallStruct, instance *instance_model.Instance) error
	Heartbeat(data *HeartbeatCallStruct, instance *instance_model.Instance) error
	ICEConfig(instance *instance_model.Instance) (ICEConfig, error)
	DeploymentDrain(data *DrainRequest) (DrainStatus, error)
}

type callService struct {
	clientPointer *registry.Clients
	runtime       *Runtime
	loggerWrapper *logger_wrapper.LoggerManager
	startInstance func(string) error
}

type DialCallStruct struct {
	Peer           string `json:"peer" binding:"required"`
	Media          string `json:"media" binding:"required"`
	IdempotencyKey string `json:"idempotencyKey" binding:"required"`
}

type AnswerCallStruct struct {
	CallID    string `json:"callId" binding:"required"`
	RequestID string `json:"requestId" binding:"required"`
}

type ControlCallStruct struct {
	CallID       string `json:"callId" binding:"required"`
	ControllerID string `json:"controllerId" binding:"required"`
}

type HeartbeatCallStruct struct {
	CallID            string             `json:"callId" binding:"required"`
	ControllerID      string             `json:"controllerId" binding:"required"`
	BrowserMediaStats *BrowserMediaStats `json:"browserMediaStats,omitempty"`
}

type BrowserMediaStats struct {
	Transport    *BrowserTransportStats    `json:"transport,omitempty"`
	InboundAudio *BrowserInboundAudioStats `json:"inboundAudio,omitempty"`
}

type BrowserTransportStats struct {
	LocalCandidateType string   `json:"localCandidateType,omitempty"`
	LocalProtocol      string   `json:"localProtocol,omitempty"`
	RelayProtocol      string   `json:"relayProtocol,omitempty"`
	CurrentRoundTripMs *float64 `json:"currentRoundTripMs,omitempty"`
}

type BrowserInboundAudioStats struct {
	AverageJitterBufferMs *float64 `json:"averageJitterBufferMs,omitempty"`
	PacketsLost           *int64   `json:"packetsLost,omitempty"`
	PacketsReceived       *uint64  `json:"packetsReceived,omitempty"`
	JitterMs              *float64 `json:"jitterMs,omitempty"`
}

type RejectCallStruct struct {
	CallCreator  types.JID `json:"callCreator,omitempty"`
	CallID       string    `json:"callId" binding:"required"`
	ControllerID string    `json:"controllerId,omitempty"`
}

type OfferCallStruct struct {
	CallID       string `json:"callId" binding:"required"`
	ControllerID string `json:"controllerId" binding:"required"`
	SDP          string `json:"sdp" binding:"required"`
}

type CandidateCallStruct struct {
	CallID           string  `json:"callId" binding:"required"`
	ControllerID     string  `json:"controllerId" binding:"required"`
	Candidate        string  `json:"candidate" binding:"required"`
	SDPMid           *string `json:"sdpMid,omitempty"`
	SDPMLine         *uint16 `json:"sdpMLineIndex,omitempty"`
	UsernameFragment *string `json:"usernameFragment,omitempty"`
}

func (c *callService) Dial(data *DialCallStruct, instance *instance_model.Instance) (CallEnvelope, error) {
	if data == nil {
		return CallEnvelope{}, errors.New("call request is required")
	}
	if len(data.IdempotencyKey) < 32 {
		return CallEnvelope{}, errors.New("idempotencyKey must be a random capability of at least 128 bits")
	}
	return c.runtime.Dial(context.Background(), instance.Id, data.Peer, strings.ToLower(data.Media), data.IdempotencyKey)
}

func (c *callService) Answer(data *AnswerCallStruct, instance *instance_model.Instance) (CallEnvelope, error) {
	if data == nil {
		return CallEnvelope{}, errors.New("call request is required")
	}
	return c.runtime.Answer(instance.Id, data.CallID, data.RequestID)
}

// RejectCall preserves the original instance-key-scoped API while adding owner-scoped call rejection.
func (c *callService) RejectCall(data *RejectCallStruct, instance *instance_model.Instance) (CallEnvelope, error) {
	if data == nil {
		return CallEnvelope{}, errors.New("call request is required")
	}
	if data.ControllerID != "" {
		return c.runtime.Control(instance.Id, data.CallID, data.ControllerID, "reject")
	}
	if data.CallCreator.IsEmpty() {
		return c.runtime.RejectUnclaimed(instance.Id, data.CallID)
	}
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return CallEnvelope{}, err
	}
	if err = client.RejectCall(context.Background(), data.CallCreator, data.CallID); err != nil {
		logger.LogError("[%s] error reject call: %v", instance.Id, err)
		return CallEnvelope{}, err
	}
	return CallEnvelope{}, nil
}

func (c *callService) Hangup(data *ControlCallStruct, instance *instance_model.Instance) (CallEnvelope, error) {
	if data == nil {
		return CallEnvelope{}, errors.New("call request is required")
	}
	return c.runtime.Control(instance.Id, data.CallID, data.ControllerID, "hangup")
}

func (c *callService) Current(instance *instance_model.Instance) (CallEnvelope, bool) {
	return c.runtime.Snapshot(instance.Id)
}

func (c *callService) CurrentMediaDiagnostics(instance *instance_model.Instance, expectedCallID string) (CallMediaDiagnostics, bool) {
	return c.runtime.CurrentMediaDiagnostics(instance.Id, expectedCallID)
}

func (c *callService) Offer(data *OfferCallStruct, instance *instance_model.Instance) (string, error) {
	if data == nil {
		return "", errors.New("call offer is required")
	}
	return c.runtime.Offer(instance.Id, data.CallID, data.ControllerID, data.SDP)
}

func (c *callService) Candidate(data *CandidateCallStruct, instance *instance_model.Instance) error {
	if data == nil {
		return errors.New("ICE candidate is required")
	}
	return c.runtime.AddCandidate(instance.Id, data.CallID, data.ControllerID, ICECandidateInit(data))
}

func ICECandidateInit(data *CandidateCallStruct) webrtc.ICECandidateInit {
	return webrtc.ICECandidateInit{Candidate: data.Candidate, SDPMid: data.SDPMid, SDPMLineIndex: data.SDPMLine, UsernameFragment: data.UsernameFragment}
}

func (c *callService) Heartbeat(data *HeartbeatCallStruct, instance *instance_model.Instance) error {
	if data == nil {
		return errors.New("call heartbeat is required")
	}
	return c.runtime.Heartbeat(instance.Id, data.CallID, data.ControllerID, data.BrowserMediaStats)
}

func (c *callService) ICEConfig(instance *instance_model.Instance) (ICEConfig, error) {
	return c.runtime.BrowserICEConfig(instance.Id)
}

func (c *callService) DeploymentDrain(data *DrainRequest) (DrainStatus, error) {
	if data == nil {
		return DrainStatus{}, errors.New("deployment drain request is required")
	}
	return c.runtime.DeploymentDrain(data.Owner, data.Action)
}

func (c *callService) ensureClientConnected(instanceID string) (*whatsmeow.Client, error) {
	client := c.clientPointer.Get(instanceID)
	if client == nil {
		if c.startInstance == nil {
			return nil, errors.New("no active session found")
		}
		if err := c.startInstance(instanceID); err != nil {
			return nil, errors.New("no active session found")
		}
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		<-timer.C
		client = c.clientPointer.Get(instanceID)
	}
	if client == nil || !client.IsConnected() {
		c.loggerWrapper.GetLogger(instanceID).LogError("[%s] call reject failed because client is disconnected", instanceID)
		return nil, errors.New("client disconnected")
	}
	return client, nil
}

func NewCallService(
	clientPointer *registry.Clients,
	runtime *Runtime,
	loggerWrapper *logger_wrapper.LoggerManager,
	startInstance func(string) error,
) CallService {
	return &callService{clientPointer: clientPointer, runtime: runtime, loggerWrapper: loggerWrapper, startInstance: startInstance}
}
