package whatsmeow_service

import (
	"testing"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
)

func TestShouldRequestInitialHistorySync(t *testing.T) {
	tests := []struct {
		name     string
		instance *instance_model.Instance
		want     bool
	}{
		{name: "new QR pairing requests the initial conversation list", instance: &instance_model.Instance{}, want: true},
		{name: "whitespace JID is still an unlinked pairing", instance: &instance_model.Instance{Jid: "  "}, want: true},
		{name: "existing linked companion does not reimport on reconnect", instance: &instance_model.Instance{Jid: "60183568788@s.whatsapp.net"}, want: false},
		{name: "nil instance fails closed", instance: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRequestInitialHistorySync(tt.instance); got != tt.want {
				t.Fatalf("shouldRequestInitialHistorySync() = %v, want %v", got, tt.want)
			}
		})
	}
}
