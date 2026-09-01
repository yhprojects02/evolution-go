package instance_repository

import (
	"database/sql"
	"fmt"
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
			repeat_count INTEGER NOT NULL DEFAULT 1,
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
	base := time.Now().UTC()
	for n := 0; n < maxDisconnectEventsPerInstance+5; n++ {
		// Distinct reasons: consecutive duplicates are collapsed by design, so
		// only distinct events can exercise the retention cap.
		if err := appender.AppendDisconnectEvent(InstanceDisconnectEvent{
			InstanceID:       instanceID,
			ClientName:       "test-client",
			EventName:        "Disconnected",
			Reason:           fmt.Sprintf("test reason %d", n),
			ConnectedAtEvent: false,
			Timestamp:        base.Add(time.Duration(n) * time.Second),
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

func TestAppendDisconnectEventCollapsesConsecutiveDuplicates(t *testing.T) {
	db, sqlDB := newInstanceRepositoryTestDB(t)
	repository := NewInstanceRepository(db)
	appender := repository.(DisconnectEventAppender)

	const instanceID = "instance-retry-loop"
	base := time.Now().UTC()
	for n := 0; n < 5; n++ {
		if err := appender.AppendDisconnectEvent(InstanceDisconnectEvent{
			InstanceID: instanceID,
			EventName:  "RelinkRequired",
			Reason:     disconnectReasonForTest,
			Jid:        "60183214788:5@s.whatsapp.net",
			Timestamp:  base.Add(time.Duration(n) * time.Minute),
		}); err != nil {
			t.Fatalf("AppendDisconnectEvent(%d) error = %v", n, err)
		}
	}

	var count, repeats int
	if err := sqlDB.QueryRow(
		`SELECT COUNT(*), COALESCE(MAX(repeat_count), 0) FROM instance_disconnect_events WHERE instance_id = ?`,
		instanceID,
	).Scan(&count, &repeats); err != nil {
		t.Fatalf("count events error = %v", err)
	}
	if count != 1 {
		t.Fatalf("event count = %d, want 1", count)
	}
	if repeats != 5 {
		t.Fatalf("repeat_count = %d, want 5", repeats)
	}
}

func TestAppendDisconnectEventKeepsLogoutReasonUnderRetryFlood(t *testing.T) {
	db, sqlDB := newInstanceRepositoryTestDB(t)
	repository := NewInstanceRepository(db)
	appender := repository.(DisconnectEventAppender)

	const instanceID = "instance-unlinked"
	const logoutReason = "401: logged out from another device"
	base := time.Now().UTC()

	if err := appender.AppendDisconnectEvent(InstanceDisconnectEvent{
		InstanceID: instanceID,
		EventName:  "LoggedOut",
		Reason:     logoutReason,
		Timestamp:  base,
	}); err != nil {
		t.Fatalf("AppendDisconnectEvent(LoggedOut) error = %v", err)
	}

	// The supervisor used to re-emit this every few minutes for days.
	for n := 0; n < maxDisconnectEventsPerInstance*3; n++ {
		if err := appender.AppendDisconnectEvent(InstanceDisconnectEvent{
			InstanceID: instanceID,
			EventName:  "RelinkRequired",
			Reason:     disconnectReasonForTest,
			Timestamp:  base.Add(time.Duration(n+1) * time.Minute),
		}); err != nil {
			t.Fatalf("AppendDisconnectEvent(RelinkRequired %d) error = %v", n, err)
		}
	}

	var kept int
	if err := sqlDB.QueryRow(
		`SELECT COUNT(*) FROM instance_disconnect_events WHERE instance_id = ? AND reason = ?`,
		instanceID, logoutReason,
	).Scan(&kept); err != nil {
		t.Fatalf("count logout events error = %v", err)
	}
	if kept != 1 {
		t.Fatalf("logout rows = %d, want 1 (the real reason was evicted by the retry loop)", kept)
	}
}
