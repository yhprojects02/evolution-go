package instance_service

import (
	"encoding/json"
	"testing"

	"github.com/EvolutionAPI/evolution-go/pkg/config"
	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	instance_repository "github.com/EvolutionAPI/evolution-go/pkg/instance/repository"
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
