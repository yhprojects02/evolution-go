package instance_model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Instance struct {
	Id               string     `json:"id" gorm:"type:uuid;primaryKey"`
	Name             string     `json:"name"`
	Token            string     `json:"token" gorm:"unique"`
	Webhook          string     `json:"webhook"`
	RabbitmqEnable   string     `json:"rabbitmqEnable"`
	WebSocketEnable  string     `json:"websocketEnable"`
	NatsEnable       string     `json:"natsEnable"`
	Jid              string     `json:"jid" gorm:"column:jid"`
	Qrcode           string     `json:"qrcode" gorm:"type:text"`
	Connected        bool       `json:"connected"`
	DisconnectedAt   *time.Time `json:"disconnectedAt,omitempty" gorm:"index"`
	Expiration       int64      `json:"expiration"`
	DisconnectReason string     `json:"disconnect_reason"`
	Events           string     `json:"events"`
	OsName           string     `json:"os_name"`
	Proxy            string     `json:"proxy"`
	ClientName       string     `json:"client_name"`
	CreatedAt        time.Time  `json:"createdAt" gorm:"autoCreateTime"`

	// Advanced Settings
	AlwaysOnline  bool   `json:"alwaysOnline" gorm:"default:false"`
	RejectCall    bool   `json:"rejectCall" gorm:"default:false"`
	MsgRejectCall string `json:"msgRejectCall" gorm:"default:''"`
	ReadMessages  bool   `json:"readMessages" gorm:"default:false"`
	IgnoreGroups  bool   `json:"ignoreGroups" gorm:"default:false"`
	IgnoreStatus  bool   `json:"ignoreStatus" gorm:"default:false"`
}

// InstanceDisconnectEvent is an append-only record of a disconnect-related
// whatsmeow event. It is migrated with Instance through the application's
// existing GORM AutoMigrate entry point.
type InstanceDisconnectEvent struct {
	Id               string     `json:"id" gorm:"type:uuid;primaryKey"`
	InstanceID       string     `json:"instance_id" gorm:"type:uuid;not null;index:idx_instance_disconnect_events_instance_timestamp,priority:1"`
	ClientName       string     `json:"client_name" gorm:"index"`
	Jid              string     `json:"jid" gorm:"column:jid"`
	EventName        string     `json:"event_name" gorm:"not null;index"`
	Reason           string     `json:"reason" gorm:"type:text"`
	ConnectedAtEvent bool       `json:"connected_at_event"`
	OnConnect        bool       `json:"on_connect"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty" gorm:"index"`
	Timestamp        time.Time  `json:"timestamp" gorm:"column:timestamp;autoCreateTime;not null;index:idx_instance_disconnect_events_instance_timestamp,priority:2"`
}

// TableName keeps the diagnostic table name explicit and stable.
func (InstanceDisconnectEvent) TableName() string {
	return "instance_disconnect_events"
}

// BeforeCreate follows Instance's UUID convention and normalizes event times
// to UTC before persistence.
func (m *InstanceDisconnectEvent) BeforeCreate(tx *gorm.DB) error {
	if m.Id == "" {
		m.Id = uuid.New().String()
	}
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now().UTC()
	} else {
		m.Timestamp = m.Timestamp.UTC()
	}
	return nil
}

// AdvancedSettings representa as configurações avançadas de uma instância
type AdvancedSettings struct {
	AlwaysOnline  bool   `json:"alwaysOnline"`
	RejectCall    bool   `json:"rejectCall"`
	MsgRejectCall string `json:"msgRejectCall"`
	ReadMessages  bool   `json:"readMessages"`
	IgnoreGroups  bool   `json:"ignoreGroups"`
	IgnoreStatus  bool   `json:"ignoreStatus"`
}

func (m *Instance) BeforeCreate(tx *gorm.DB) (err error) {
	if m.Id == "" {
		m.Id = uuid.New().String()
	}
	return
}
