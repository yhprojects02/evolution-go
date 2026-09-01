package whatsmeow_service

import (
	"strings"
	"testing"

	"github.com/EvolutionAPI/evolution-go/pkg/config"
	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	instance_repository "github.com/EvolutionAPI/evolution-go/pkg/instance/repository"
	logger_wrapper "github.com/EvolutionAPI/evolution-go/pkg/logger"
	"github.com/EvolutionAPI/evolution-go/pkg/registry"
	"github.com/patrickmn/go-cache"
)

// reconnectRepositoryStub records every write ReconnectClient attempts so a
// test can prove the specific disconnect reason is never replaced by the
// generic "Reconnecting" marker.
type reconnectRepositoryStub struct {
	instance_repository.InstanceRepository
	instance         *instance_model.Instance
	blindReasons     []string
	conditionalCalls []string
}

func (r *reconnectRepositoryStub) GetInstanceByID(_ string) (*instance_model.Instance, error) {
	copied := *r.instance
	return &copied, nil
}

func (r *reconnectRepositoryStub) UpdateConnected(_ string, _ bool, reason string) error {
	r.blindReasons = append(r.blindReasons, reason)
	return nil
}

func (r *reconnectRepositoryStub) UpdateConnectedIfReasonEmptyOrGeneric(_ string, genericReason string) error {
	r.conditionalCalls = append(r.conditionalCalls, genericReason)
	return nil
}

func newReconnectTestService(t *testing.T, repository instance_repository.InstanceRepository) whatsmeowService {
	t.Helper()

	cfg := &config.Config{LogDirectory: t.TempDir()}
	return whatsmeowService{
		instanceRepository: repository,
		config:             cfg,
		killChannel:        registry.NewKillChannels(),
		userInfoCache:      cache.New(cache.NoExpiration, cache.NoExpiration),
		clientPointer:      registry.NewClients(),
		myClientPointer:    newMyClients(),
		loggerWrapper:      logger_wrapper.NewLoggerManager(cfg),
	}
}

func TestReconnectClientRefusesTerminalDisconnectReason(t *testing.T) {
	const instanceID = "instance-unlinked"
	repository := &reconnectRepositoryStub{
		instance: &instance_model.Instance{
			Id:               instanceID,
			Jid:              "60183214788:5@s.whatsapp.net",
			DisconnectReason: "401: logged out from another device",
		},
	}
	service := newReconnectTestService(t, repository)

	err := service.ReconnectClient(instanceID)
	if err == nil {
		t.Fatal("ReconnectClient() error = nil, want refusal for a revoked companion")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("ReconnectClient() error = %q, want it to carry the real reason", err)
	}
	if len(repository.blindReasons) != 0 || len(repository.conditionalCalls) != 0 {
		t.Fatalf("disconnect reason was rewritten: blind=%v conditional=%v",
			repository.blindReasons, repository.conditionalCalls)
	}
}

func TestReconnectClientPreservesReasonThroughConditionalUpdate(t *testing.T) {
	const instanceID = "instance-flapping"
	repository := &reconnectRepositoryStub{
		instance: &instance_model.Instance{
			Id:               instanceID,
			Jid:              "60123456789:3@s.whatsapp.net",
			DisconnectReason: "Disconnected",
		},
	}
	service := newReconnectTestService(t, repository)

	// StartInstance needs infrastructure this unit test does not build, so the
	// call is expected to fail after the status write under test.
	_ = service.ReconnectClient(instanceID)

	if len(repository.blindReasons) != 0 {
		t.Fatalf("blind UpdateConnected called with %v, want the conditional writer", repository.blindReasons)
	}
	if len(repository.conditionalCalls) != 1 || repository.conditionalCalls[0] != "Reconnecting" {
		t.Fatalf("conditional calls = %v, want one call with %q", repository.conditionalCalls, "Reconnecting")
	}
}
