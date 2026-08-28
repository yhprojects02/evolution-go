package instance_repository

import instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"

// maxDisconnectEventsPerInstance keeps a flapping instance from growing this
// append-only table without bound while retaining the latest 100 events for
// incident investigation.
const maxDisconnectEventsPerInstance = 100

// InstanceDisconnectEvent is kept as a repository-level alias for callers
// that already construct repository-owned rows. The actual model remains in
// the instance model package so the application's existing migration entry
// point can register it alongside Instance.
type InstanceDisconnectEvent = instance_model.InstanceDisconnectEvent
