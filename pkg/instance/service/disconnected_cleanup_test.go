package instance_service

import (
	"testing"
	"time"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
)

func TestIsExpiredDisconnected(t *testing.T) {
	cutoff := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	beforeCutoff := cutoff.Add(-time.Second)
	afterCutoff := cutoff.Add(time.Second)

	tests := []struct {
		name     string
		instance *instance_model.Instance
		want     bool
	}{
		{
			name:     "positive workflow older than retention",
			instance: &instance_model.Instance{Connected: false, DisconnectedAt: &beforeCutoff},
			want:     true,
		},
		{
			name:     "boundary exactly at cutoff",
			instance: &instance_model.Instance{Connected: false, DisconnectedAt: &cutoff},
			want:     true,
		},
		{
			name:     "inside retention window",
			instance: &instance_model.Instance{Connected: false, DisconnectedAt: &afterCutoff},
			want:     false,
		},
		{
			name:     "connected instance is protected",
			instance: &instance_model.Instance{Connected: true, DisconnectedAt: &beforeCutoff},
			want:     false,
		},
		{
			name:     "empty disconnect timestamp is protected",
			instance: &instance_model.Instance{Connected: false, DisconnectedAt: nil},
			want:     false,
		},
		{
			name:     "nil instance is protected",
			instance: nil,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isExpiredDisconnected(tt.instance, cutoff); got != tt.want {
				t.Fatalf("isExpiredDisconnected() = %v, want %v", got, tt.want)
			}
		})
	}
}
