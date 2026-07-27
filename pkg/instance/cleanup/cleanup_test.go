package instance_cleanup

import (
	"errors"
	"testing"
	"time"

	instance_service "github.com/EvolutionAPI/evolution-go/pkg/instance/service"
)

type fakeCleanupService struct {
	gotNow       time.Time
	gotRetention time.Duration
	result       instance_service.CleanupResult
	err          error
}

func (f *fakeCleanupService) CleanupExpiredDisconnected(now time.Time, retention time.Duration) (instance_service.CleanupResult, error) {
	f.gotNow = now
	f.gotRetention = retention
	return f.result, f.err
}

func TestCleanerSweep(t *testing.T) {
	expectedNow := time.Date(2026, 7, 27, 12, 0, 0, 0, time.FixedZone("MYT", 8*60*60))
	expectedResult := instance_service.CleanupResult{TrackingInitialized: 2, Deleted: 3}
	service := &fakeCleanupService{result: expectedResult}
	cleaner, err := New(service, DefaultRetention, DefaultInterval)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cleaner.now = func() time.Time { return expectedNow }

	got, err := cleaner.Sweep()
	if err != nil {
		t.Fatalf("Sweep() error = %v", err)
	}
	if got != expectedResult {
		t.Fatalf("Sweep() result = %+v, want %+v", got, expectedResult)
	}
	if service.gotNow != expectedNow.UTC() {
		t.Fatalf("cleanup now = %s, want UTC %s", service.gotNow, expectedNow.UTC())
	}
	if service.gotRetention != 7*24*time.Hour {
		t.Fatalf("cleanup retention = %s, want 168h", service.gotRetention)
	}
}

func TestCleanerSweepReturnsServiceException(t *testing.T) {
	expectedError := errors.New("database unavailable")
	expectedResult := instance_service.CleanupResult{Deleted: 1, Failed: 1}
	service := &fakeCleanupService{result: expectedResult, err: expectedError}
	cleaner, err := New(service, DefaultRetention, DefaultInterval)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	got, err := cleaner.Sweep()
	if !errors.Is(err, expectedError) {
		t.Fatalf("Sweep() error = %v, want %v", err, expectedError)
	}
	if got != expectedResult {
		t.Fatalf("Sweep() result = %+v, want %+v", got, expectedResult)
	}
}

func TestNewRejectsNullAndBoundaryConfiguration(t *testing.T) {
	validService := &fakeCleanupService{}
	tests := []struct {
		name      string
		service   CleanupService
		retention time.Duration
		interval  time.Duration
	}{
		{name: "nil service", service: nil, retention: DefaultRetention, interval: DefaultInterval},
		{name: "zero retention", service: validService, retention: 0, interval: DefaultInterval},
		{name: "negative retention", service: validService, retention: -time.Second, interval: DefaultInterval},
		{name: "zero interval", service: validService, retention: DefaultRetention, interval: 0},
		{name: "negative interval", service: validService, retention: DefaultRetention, interval: -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.service, tt.retention, tt.interval); err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
		})
	}
}
