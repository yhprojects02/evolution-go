package instance_service

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/EvolutionAPI/evolution-go/pkg/config"
	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	instance_repository "github.com/EvolutionAPI/evolution-go/pkg/instance/repository"
	logger_wrapper "github.com/EvolutionAPI/evolution-go/pkg/logger"
	"github.com/EvolutionAPI/evolution-go/pkg/registry"
	whatsmeow_service "github.com/EvolutionAPI/evolution-go/pkg/whatsmeow/service"
)

type createInstanceRepositoryStub struct {
	instance_repository.InstanceRepository
	created *instance_model.Instance
}

func (r *createInstanceRepositoryStub) GetInstanceByName(string) (*instance_model.Instance, error) {
	return nil, nil
}

func (r *createInstanceRepositoryStub) Create(instance instance_model.Instance) (*instance_model.Instance, error) {
	r.created = &instance
	return r.created, nil
}

func newCreateServiceForTest(repository instance_repository.InstanceRepository) instances {
	return instances{
		instanceRepository: repository,
		config:             &config.Config{OsName: "Configured OS"},
	}
}

func assertCreatedOSName(t *testing.T, repository *createInstanceRepositoryStub, want string) {
	t.Helper()
	if repository.created == nil {
		t.Fatal("repository did not receive an instance")
	}
	if repository.created.OsName != want {
		t.Fatalf("repository received OsName = %q, want %q", repository.created.OsName, want)
	}
}

func TestCreateUsesSuppliedOSName(t *testing.T) {
	var data CreateStruct
	if err := json.Unmarshal([]byte(`{"name":"instance","token":"token","os_name":"Supplied OS"}`), &data); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	repository := &createInstanceRepositoryStub{}
	created, err := newCreateServiceForTest(repository).Create(&data)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.OsName != "Supplied OS" {
		t.Fatalf("created OsName = %q, want %q", created.OsName, "Supplied OS")
	}
	assertCreatedOSName(t, repository, "Supplied OS")
}

func TestCreateFallsBackToConfiguredOSNameWhenAbsent(t *testing.T) {
	var data CreateStruct
	if err := json.Unmarshal([]byte(`{"name":"instance","token":"token"}`), &data); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	repository := &createInstanceRepositoryStub{}
	created, err := newCreateServiceForTest(repository).Create(&data)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.OsName != "Configured OS" {
		t.Fatalf("created OsName = %q, want %q", created.OsName, "Configured OS")
	}
	assertCreatedOSName(t, repository, "Configured OS")
}

type connectInstanceRepositoryStub struct {
	instance_repository.InstanceRepository
}

func (r *connectInstanceRepositoryStub) Update(*instance_model.Instance) error {
	return nil
}

type connectWhatsmeowServiceStub struct {
	whatsmeow_service.WhatsmeowService
	started chan *whatsmeow_service.ClientData
}

func (s *connectWhatsmeowServiceStub) UpdateInstanceSettings(string) error {
	return errors.New("instance is not running")
}

func (s *connectWhatsmeowServiceStub) StartClient(data *whatsmeow_service.ClientData) {
	s.started <- data
}

func TestConnectRequiresExplicitAllowNewDeviceOptIn(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "omitted field is safe", payload: `{}`, want: false},
		{name: "explicit login opt-in", payload: `{"allowNewDevice":true}`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var data ConnectStruct
			if err := json.Unmarshal([]byte(tt.payload), &data); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}

			started := make(chan *whatsmeow_service.ClientData, 1)
			service := instances{
				instanceRepository: &connectInstanceRepositoryStub{},
				config:             &config.Config{LogDirectory: t.TempDir()},
				killChannel:        registry.NewKillChannels(),
				clientPointer:      registry.NewClients(),
				whatsmeowService:   &connectWhatsmeowServiceStub{started: started},
				loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: t.TempDir()}),
				loginLocks:         newKeyedMutex(),
			}
			instance := &instance_model.Instance{Id: "connect-opt-in-test"}

			if _, _, _, err := service.Connect(&data, instance); err != nil {
				t.Fatalf("Connect() error = %v", err)
			}

			select {
			case clientData := <-started:
				if clientData.AllowNewDevice != tt.want {
					t.Fatalf("ClientData.AllowNewDevice = %v, want %v", clientData.AllowNewDevice, tt.want)
				}
			case <-time.After(time.Second):
				t.Fatal("Connect() did not start the client")
			}
		})
	}
}
