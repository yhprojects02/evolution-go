package whatsmeow_service

import (
	"context"
	"testing"
	"time"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	"github.com/EvolutionAPI/evolution-go/pkg/registry"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

type deviceStoreContainerStub struct {
	newDevice      *store.Device
	newDeviceCalls int
}

func (s *deviceStoreContainerStub) GetDevice(context.Context, types.JID) (*store.Device, error) {
	return nil, nil
}

func (s *deviceStoreContainerStub) NewDevice() *store.Device {
	s.newDeviceCalls++
	return s.newDevice
}

func TestLoadDeviceStoreReconnectWithEmptyJIDDoesNotMintDevice(t *testing.T) {
	container := &deviceStoreContainerStub{newDevice: &store.Device{}}

	got, err := loadDeviceStore(
		context.Background(),
		container,
		&instance_model.Instance{},
		false,
	)
	if err != nil {
		t.Fatalf("loadDeviceStore() error = %v", err)
	}
	if got != nil {
		t.Fatal("loadDeviceStore() returned a device for a reconnect with no JID")
	}
	if container.newDeviceCalls != 0 {
		t.Fatalf("NewDevice() calls = %d, want 0", container.newDeviceCalls)
	}
}

func TestLoadDeviceStoreDeliberateLoginWithEmptyJIDMintsDevice(t *testing.T) {
	want := &store.Device{}
	container := &deviceStoreContainerStub{newDevice: want}

	got, err := loadDeviceStore(
		context.Background(),
		container,
		&instance_model.Instance{},
		true,
	)
	if err != nil {
		t.Fatalf("loadDeviceStore() error = %v", err)
	}
	if got != want {
		t.Fatalf("loadDeviceStore() device = %p, want %p", got, want)
	}
	if container.newDeviceCalls != 1 {
		t.Fatalf("NewDevice() calls = %d, want 1", container.newDeviceCalls)
	}
}

func TestLoadDeviceStoreReconnectWithExistingJIDDoesNotMintDevice(t *testing.T) {
	container := &deviceStoreContainerStub{newDevice: &store.Device{}}

	got, err := loadDeviceStore(
		context.Background(),
		container,
		&instance_model.Instance{Jid: "60183214788:4"},
		false,
	)
	if err != nil {
		t.Fatalf("loadDeviceStore() error = %v", err)
	}
	if got != nil {
		t.Fatal("loadDeviceStore() returned a device for a reconnect with missing credentials")
	}
	if container.newDeviceCalls != 0 {
		t.Fatalf("NewDevice() calls = %d, want 0", container.newDeviceCalls)
	}
}

func TestQRLoopStopSignalWakesWithoutBlockingDeleter(t *testing.T) {
	const instanceID = "instance-qr-stop-test"
	kills := registry.NewKillChannels()
	stop := kills.Reset(instanceID)
	qrChan := make(chan whatsmeow.QRChannelItem)

	type receiveResult struct {
		channelOpen bool
		stopped     bool
	}
	received := make(chan receiveResult, 1)
	go func() {
		_, channelOpen, stopped := nextQRChannelItem(stop, qrChan)
		received <- receiveResult{channelOpen: channelOpen, stopped: stopped}
	}()

	deleted := make(chan struct{})
	go func() {
		kills.Signal(instanceID)
		kills.Delete(instanceID)
		close(deleted)
	}()

	select {
	case <-deleted:
	case <-time.After(time.Second):
		t.Fatal("deletion blocked while signaling the QR loop")
	}

	select {
	case result := <-received:
		if !result.stopped || result.channelOpen {
			t.Fatalf("QR receive result = %+v, want stopped=true and channelOpen=false", result)
		}
	case <-time.After(time.Second):
		t.Fatal("QR loop did not observe the deletion stop signal")
	}
}
