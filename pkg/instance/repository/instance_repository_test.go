package instance_repository

import (
	"database/sql"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	_ "modernc.org/sqlite"
)

func newInstanceRepositoryTestDB(t *testing.T) (*gorm.DB, *sql.DB) {
	t.Helper()

	sqlDB, err := sql.Open("sqlite", "file:instance-repository-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:             sqlDB,
		WithoutReturning: true,
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}

	statements := []string{
		`CREATE TABLE instances (
			id TEXT PRIMARY KEY,
			connected BOOLEAN NOT NULL DEFAULT FALSE,
			disconnect_reason TEXT,
			disconnected_at TIMESTAMP
		)`,
		`CREATE TABLE instance_disconnect_events (
			id TEXT PRIMARY KEY,
			instance_id TEXT NOT NULL,
			client_name TEXT,
			jid TEXT,
			event_name TEXT NOT NULL,
			reason TEXT,
			connected_at_event BOOLEAN NOT NULL DEFAULT FALSE,
			on_connect BOOLEAN NOT NULL DEFAULT FALSE,
			expires_at TIMESTAMP,
			timestamp TIMESTAMP NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := sqlDB.Exec(statement); err != nil {
			t.Fatalf("schema setup error = %v", err)
		}
	}

	return db, sqlDB
}

func TestUpdateConnectedIfReasonEmptyOrGenericPreservesSpecificReason(t *testing.T) {
	db, sqlDB := newInstanceRepositoryTestDB(t)
	repository := NewInstanceRepository(db)
	updater, ok := repository.(ConditionalConnectedUpdater)
	if !ok {
		t.Fatal("instance repository does not implement ConditionalConnectedUpdater")
	}

	const instanceID = "instance-specific"
	if _, err := sqlDB.Exec(
		`INSERT INTO instances (id, connected, disconnect_reason) VALUES (?, ?, ?)`,
		instanceID,
		true,
		"specific WhatsApp reason",
	); err != nil {
		t.Fatalf("insert instance error = %v", err)
	}

	if err := updater.UpdateConnectedIfReasonEmptyOrGeneric(instanceID, disconnectReasonForTest); err != nil {
		t.Fatalf("UpdateConnectedIfReasonEmptyOrGeneric() error = %v", err)
	}

	var reason string
	var connected bool
	if err := sqlDB.QueryRow(`SELECT disconnect_reason, connected FROM instances WHERE id = ?`, instanceID).Scan(&reason, &connected); err != nil {
		t.Fatalf("query instance error = %v", err)
	}
	if reason != "specific WhatsApp reason" {
		t.Fatalf("disconnect_reason = %q, want specific reason preserved", reason)
	}
	if connected {
		t.Fatal("connected = true, want false after relink-required update")
	}
}

func TestUpdateConnectedIfReasonEmptyOrGenericWritesGenericReasonWhenEmpty(t *testing.T) {
	db, sqlDB := newInstanceRepositoryTestDB(t)
	repository := NewInstanceRepository(db)
	updater, ok := repository.(ConditionalConnectedUpdater)
	if !ok {
		t.Fatal("instance repository does not implement ConditionalConnectedUpdater")
	}

	const instanceID = "instance-empty"
	if _, err := sqlDB.Exec(
		`INSERT INTO instances (id, connected, disconnect_reason) VALUES (?, ?, ?)`,
		instanceID,
		true,
		"",
	); err != nil {
		t.Fatalf("insert instance error = %v", err)
	}

	if err := updater.UpdateConnectedIfReasonEmptyOrGeneric(instanceID, disconnectReasonForTest); err != nil {
		t.Fatalf("UpdateConnectedIfReasonEmptyOrGeneric() error = %v", err)
	}

	var reason string
	if err := sqlDB.QueryRow(`SELECT disconnect_reason FROM instances WHERE id = ?`, instanceID).Scan(&reason); err != nil {
		t.Fatalf("query instance error = %v", err)
	}
	if reason != disconnectReasonForTest {
		t.Fatalf("disconnect_reason = %q, want %q", reason, disconnectReasonForTest)
	}
}

func TestUpdateConnectedIfReasonEmptyOrGenericPreservesSpecificReasonFromQRLimit(t *testing.T) {
	db, sqlDB := newInstanceRepositoryTestDB(t)
	repository := NewInstanceRepository(db)
	updater, ok := repository.(ConditionalConnectedUpdater)
	if !ok {
		t.Fatal("instance repository does not implement ConditionalConnectedUpdater")
	}

	const instanceID = "instance-qr-limit"
	const qrLimitReason = "QR code limit reached (5)"
	const specificReason = "401: logged out from another device"
	if _, err := sqlDB.Exec(
		`INSERT INTO instances (id, connected, disconnect_reason) VALUES (?, ?, ?)`,
		instanceID,
		true,
		specificReason,
	); err != nil {
		t.Fatalf("insert instance error = %v", err)
	}

	if err := updater.UpdateConnectedIfReasonEmptyOrGeneric(instanceID, qrLimitReason); err != nil {
		t.Fatalf("UpdateConnectedIfReasonEmptyOrGeneric() error = %v", err)
	}

	var reason string
	var connected bool
	if err := sqlDB.QueryRow(`SELECT disconnect_reason, connected FROM instances WHERE id = ?`, instanceID).Scan(&reason, &connected); err != nil {
		t.Fatalf("query instance error = %v", err)
	}
	if reason != specificReason {
		t.Fatalf("disconnect_reason = %q, want specific reason preserved", reason)
	}
	if connected {
		t.Fatal("connected = true, want false after QR-limit update")
	}
}

func TestAppendDisconnectEventCapsRowsPerInstance(t *testing.T) {
	db, sqlDB := newInstanceRepositoryTestDB(t)
	repository := NewInstanceRepository(db)
	appender, ok := repository.(DisconnectEventAppender)
	if !ok {
		t.Fatal("instance repository does not implement DisconnectEventAppender")
	}

	const instanceID = "instance-flapping"
	for n := 0; n < maxDisconnectEventsPerInstance+5; n++ {
		if err := appender.AppendDisconnectEvent(InstanceDisconnectEvent{
			InstanceID:       instanceID,
			ClientName:       "test-client",
			EventName:        "Disconnected",
			Reason:           "test reason",
			ConnectedAtEvent: false,
			Timestamp:        time.Now().UTC(),
		}); err != nil {
			t.Fatalf("AppendDisconnectEvent(%d) error = %v", n, err)
		}
	}

	var count int
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM instance_disconnect_events WHERE instance_id = ?`, instanceID).Scan(&count); err != nil {
		t.Fatalf("count events error = %v", err)
	}
	if count != maxDisconnectEventsPerInstance {
		t.Fatalf("event count = %d, want %d", count, maxDisconnectEventsPerInstance)
	}
}

const disconnectReasonForTest = "relink required: test generic reason"
