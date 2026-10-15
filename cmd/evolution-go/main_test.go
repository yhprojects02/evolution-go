package main

import (
	"encoding/json"
	"strings"
	"testing"

	call_service "github.com/EvolutionAPI/evolution-go/pkg/call/service"
	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
)

func TestCallStateWebhookPayloadKeepsAuthMetadataOutsideEnvelope(t *testing.T) {
	instance := &instance_model.Instance{Id: "instance-uuid", Name: "account", Token: "instance-secret"}
	event := call_service.CallEnvelope{
		CallID:       "call-1",
		InstanceID:   instance.Id,
		State:        call_service.CallStateActive,
		ControllerID: "controller-secret",
	}
	encoded, err := json.Marshal(buildCallStateWebhookPayload(instance, event))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	assertStringField := func(name, want string) {
		t.Helper()
		var got string
		if err := json.Unmarshal(payload[name], &got); err != nil || got != want {
			t.Fatalf("payload.%s = %q (unmarshal error %v), want %q", name, got, err, want)
		}
	}
	assertStringField("event", "CallState")
	assertStringField("instance", instance.Id)
	assertStringField("instanceId", instance.Id)
	assertStringField("instanceName", instance.Name)
	assertStringField("instanceToken", instance.Token)

	dataRaw, ok := payload["data"]
	if !ok {
		t.Fatal("CallState payload has no data envelope")
	}
	var data map[string]json.RawMessage
	if err = json.Unmarshal(dataRaw, &data); err != nil {
		t.Fatal(err)
	}
	if _, ok = data["controllerId"]; ok {
		t.Fatal("CallState envelope exposed controller capability")
	}
	if strings.Contains(string(dataRaw), instance.Token) || strings.Contains(string(dataRaw), event.ControllerID) {
		t.Fatal("CallState envelope contains webhook or controller secret")
	}
}
