package whatsmeow_service

import (
	"errors"
	"testing"
	"time"

	"github.com/EvolutionAPI/evolution-go/pkg/config"
	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	instance_repository "github.com/EvolutionAPI/evolution-go/pkg/instance/repository"
	logger_wrapper "github.com/EvolutionAPI/evolution-go/pkg/logger"
	"github.com/EvolutionAPI/evolution-go/pkg/registry"
	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow/types/events"
)

type disconnectEventRepositoryStub struct {
	instance_repository.InstanceRepository
	events       []instance_repository.InstanceDisconnectEvent
	appendErr    error
	connected    []bool
	reasons      []string
	updateErrors []error
}

func (r *disconnectEventRepositoryStub) AppendDisconnectEvent(event instance_repository.InstanceDisconnectEvent) error {
	if r.appendErr != nil {
		return r.appendErr
	}
	r.events = append(r.events, event)
	return nil
}

func (r *disconnectEventRepositoryStub) UpdateConnected(_ string, connected bool, reason string) error {
	r.connected = append(r.connected, connected)
	r.reasons = append(r.reasons, reason)
	if len(r.updateErrors) == 0 {
		return nil
	}
	err := r.updateErrors[0]
	r.updateErrors = r.updateErrors[1:]
	return err
}

type disconnectEventServiceStub struct {
	WhatsmeowService
	webhookCalls chan struct{}
}

func (s *disconnectEventServiceStub) CallWebhook(_ *instance_model.Instance, _ string, _ []byte) {
	s.webhookCalls <- struct{}{}
}

func newDisconnectEventTestClient(t *testing.T, repository instance_repository.InstanceRepository) (*MyClient, *disconnectEventServiceStub) {
	t.Helper()

	const instanceID = "instance-disconnect-event-test"
	cfg := &config.Config{LogDirectory: t.TempDir()}
	killChannels := registry.NewKillChannels()
	killChannels.Ensure(instanceID)
	service := &disconnectEventServiceStub{webhookCalls: make(chan struct{}, 4)}
	client := &MyClient{
		service:            service,
		userID:             instanceID,
		Instance:           &instance_model.Instance{Id: instanceID, ClientName: "test-client", Jid: "60123456789:3@s.whatsapp.net", Token: "token"},
		instanceRepository: repository,
		userInfoCache:      cache.New(cache.NoExpiration, cache.NoExpiration),
		killChannel:        killChannels,
		config:             cfg,
		loggerWrapper:      logger_wrapper.NewLoggerManager(cfg),
	}
	return client, service
}

func TestMyEventHandlerRecordsDisconnectEvents(t *testing.T) {
	tests := []struct {
		name          string
		event         interface{}
		wantName      string
		wantReason    string
		wantConnected bool
		wantOnConnect bool
	}{
		{
			name:          "LoggedOut",
			event:         &events.LoggedOut{OnConnect: true, Reason: events.ConnectFailureLoggedOut},
			wantName:      "LoggedOut",
			wantReason:    events.ConnectFailureLoggedOut.String(),
			wantConnected: true,
			wantOnConnect: true,
		},
		{
			name:          "ConnectFailure",
			event:         &events.ConnectFailure{Reason: events.ConnectFailureGeneric},
			wantName:      "ConnectFailure",
			wantReason:    events.ConnectFailureGeneric.String(),
			wantConnected: true,
		},
		{
			name:          "StreamReplaced",
			event:         &events.StreamReplaced{},
			wantName:      "StreamReplaced",
			wantReason:    disconnectReasonStreamReplaced,
			wantConnected: true,
		},
		{
			name:          "Disconnected",
			event:         &events.Disconnected{},
			wantName:      "Disconnected",
			wantReason:    "Disconnected emitted because the websocket is closed by the server.",
			wantConnected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &disconnectEventRepositoryStub{}
			client, service := newDisconnectEventTestClient(t, repository)
			client.Instance.Connected = true

			client.myEventHandler(tt.event)

			if len(repository.events) != 1 {
				t.Fatalf("recorded %d disconnect events, want 1", len(repository.events))
			}
			recorded := repository.events[0]
			if recorded.EventName != tt.wantName {
				t.Errorf("event name = %q, want %q", recorded.EventName, tt.wantName)
			}
			if recorded.Reason != tt.wantReason {
				t.Errorf("reason = %q, want %q", recorded.Reason, tt.wantReason)
			}
			if recorded.ConnectedAtEvent != tt.wantConnected {
				t.Errorf("connected_at_event = %v, want %v", recorded.ConnectedAtEvent, tt.wantConnected)
			}
			if recorded.OnConnect != tt.wantOnConnect {
				t.Errorf("on_connect = %v, want %v", recorded.OnConnect, tt.wantOnConnect)
			}
			if recorded.ClientName != "test-client" || recorded.Jid != "60123456789:3@s.whatsapp.net" {
				t.Errorf("identity = (%q, %q), want test-client and test JID", recorded.ClientName, recorded.Jid)
			}
			if len(repository.connected) != 1 || repository.connected[0] {
				t.Errorf("connected updates = %v, want one false update", repository.connected)
			}

			select {
			case <-service.webhookCalls:
			case <-time.After(time.Second):
				t.Fatal("disconnect event was not dispatched to the webhook")
			}
		})
	}
}

func TestMyEventHandlerSwallowsDisconnectEventWriteFailure(t *testing.T) {
	repository := &disconnectEventRepositoryStub{appendErr: errors.New("diagnostic database unavailable")}
	client, service := newDisconnectEventTestClient(t, repository)
	client.Instance.Connected = true

	// myEventHandler is void by design. A failed diagnostic write must not
	// escape it or prevent the normal disconnect update from running.
	client.myEventHandler(&events.StreamReplaced{})

	if len(repository.events) != 0 {
		t.Fatalf("recorded %d events after append failure, want 0", len(repository.events))
	}
	if len(repository.connected) != 1 || repository.connected[0] {
		t.Errorf("connected updates = %v, want one false update", repository.connected)
	}
	select {
	case <-service.webhookCalls:
	case <-time.After(time.Second):
		t.Fatal("event handler stopped before dispatching the webhook")
	}
}

func TestMyEventHandlerRecordsTemporaryBanExpiry(t *testing.T) {
	repository := &disconnectEventRepositoryStub{}
	client, _ := newDisconnectEventTestClient(t, repository)
	client.Instance.Connected = true
	before := time.Now().UTC().Add(59 * time.Minute)

	client.myEventHandler(&events.TemporaryBan{
		Code:   events.TempBanSentToTooManyPeople,
		Expire: time.Hour,
	})

	if len(repository.events) != 1 {
		t.Fatalf("recorded %d disconnect events, want 1", len(repository.events))
	}
	recorded := repository.events[0]
	if recorded.EventName != "TemporaryBan" {
		t.Fatalf("event name = %q, want TemporaryBan", recorded.EventName)
	}
	if recorded.Reason != events.TempBanSentToTooManyPeople.String() {
		t.Fatalf("reason = %q, want %q", recorded.Reason, events.TempBanSentToTooManyPeople.String())
	}
	if recorded.ExpiresAt == nil || recorded.ExpiresAt.Before(before) {
		t.Fatalf("expires_at = %v, want roughly one hour from now", recorded.ExpiresAt)
	}
}
