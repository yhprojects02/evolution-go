package instance_repository

import (
	"errors"
	"fmt"
	"time"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	"github.com/gomessguii/logger"
	"github.com/google/uuid"
	"gorm.io/gorm"

	label_model "github.com/EvolutionAPI/evolution-go/pkg/label/model"
	label_repository "github.com/EvolutionAPI/evolution-go/pkg/label/repository"

	message_model "github.com/EvolutionAPI/evolution-go/pkg/message/model"
	message_repository "github.com/EvolutionAPI/evolution-go/pkg/message/repository"
)

type InstanceRepository interface {
	Create(instance instance_model.Instance) (*instance_model.Instance, error)
	GetInstanceByID(instanceId string) (*instance_model.Instance, error)
	GetConnectedInstanceByID(instanceId string) (*instance_model.Instance, error)
	GetInstanceByToken(token string) (*instance_model.Instance, error)
	GetInstanceByName(name string) (*instance_model.Instance, error)
	Update(*instance_model.Instance) error
	UpdateConnected(userId string, status bool, disconnectReason string) error
	InitializeDisconnectedTracking(now time.Time, clientName string) (int64, error)
	GetDisconnectedBefore(cutoff time.Time, clientName string) ([]*instance_model.Instance, error)
	UpdateQrcode(userId string, qr string) error
	UpdateProxy(userId string, proxy string) error
	UpdateJid(userId string, jid string) error
	GetAllConnectedInstances() ([]*instance_model.Instance, error)
	GetAllConnectedInstancesByClientName(clientName string) ([]*instance_model.Instance, error)
	GetAll(clientName string) ([]*instance_model.Instance, error)
	Delete(instanceId string) error
	GetAdvancedSettings(instanceId string) (*instance_model.AdvancedSettings, error)
	UpdateAdvancedSettings(instanceId string, settings *instance_model.AdvancedSettings) error
}

// ConditionalConnectedUpdater is implemented by repositories that can
// preserve a specific disconnect reason with one SQL UPDATE predicate.
// Keeping it separate from InstanceRepository avoids breaking existing test
// doubles and integrations that only need the original instance operations.
type ConditionalConnectedUpdater interface {
	UpdateConnectedIfReasonEmptyOrGeneric(userId string, genericReason string) error
}

// DisconnectEventAppender persists append-only disconnect diagnostics.
// Event callers deliberately type-assert this optional capability so a
// persistence failure can be logged and swallowed without stopping WhatsApp.
type DisconnectEventAppender interface {
	AppendDisconnectEvent(event InstanceDisconnectEvent) error
}

type instanceRepository struct {
	db          *gorm.DB
	labelRepo   label_repository.LabelRepository
	messageRepo message_repository.MessageRepository
}

func (i *instanceRepository) Create(instance instance_model.Instance) (*instance_model.Instance, error) {
	if !instance.Connected && instance.DisconnectedAt == nil {
		now := time.Now().UTC()
		instance.DisconnectedAt = &now
	}
	if err := i.db.Create(&instance).Error; err != nil {
		return nil, err
	}
	return &instance, nil
}

func (i *instanceRepository) GetInstanceByToken(token string) (*instance_model.Instance, error) {
	var instance instance_model.Instance
	err := i.db.Where("token = ?", token).First(&instance).Error
	if err != nil {
		return nil, err
	}

	return &instance, nil
}

func (i *instanceRepository) GetInstanceByName(name string) (*instance_model.Instance, error) {
	var instance instance_model.Instance
	err := i.db.Where("name = ?", name).First(&instance).Error
	if err != nil {
		return nil, err
	}

	return &instance, nil
}

func (i *instanceRepository) GetInstanceByID(instanceId string) (*instance_model.Instance, error) {
	// Valida o formato do UUID
	if _, err := uuid.Parse(instanceId); err != nil {
		return nil, fmt.Errorf("invalid UUID format: %v", err)
	}

	var instance instance_model.Instance
	err := i.db.Where("id = ?", instanceId).First(&instance).Error
	if err != nil {
		return nil, err
	}

	return &instance, nil
}

func (i *instanceRepository) GetConnectedInstanceByID(instanceId string) (*instance_model.Instance, error) {
	var instance instance_model.Instance
	err := i.db.Where("id = ? AND connected = ?", instanceId, true).First(&instance).Error
	if err != nil {
		return nil, err
	}

	return &instance, nil
}

func (i *instanceRepository) Update(instance *instance_model.Instance) error {
	if instance.Connected {
		instance.DisconnectedAt = nil
	} else if instance.DisconnectedAt == nil {
		now := time.Now().UTC()
		instance.DisconnectedAt = &now
	}
	err := i.db.Save(&instance).Error
	if err != nil {
		logger.LogError("Error updating instance in DB: %v", err)
	}
	return err
}

func (i *instanceRepository) UpdateConnected(userId string, status bool, disconnectReason string) error {
	updates := map[string]interface{}{
		"connected":         status,
		"disconnect_reason": disconnectReason,
	}
	if status {
		updates["disconnected_at"] = nil
	} else {
		updates["disconnected_at"] = gorm.Expr("COALESCE(disconnected_at, ?)", time.Now().UTC())
	}
	return i.db.Model(&instance_model.Instance{}).Where("id = ?", userId).Updates(updates).Error
}

// UpdateConnectedIfReasonEmptyOrGeneric records the relink-required state
// without replacing a more specific reason that may have arrived concurrently.
// The predicate and update are one SQL statement, so this is deliberately not
// implemented as a read followed by UpdateConnected.
func (i *instanceRepository) UpdateConnectedIfReasonEmptyOrGeneric(userId string, genericReason string) error {
	updates := map[string]interface{}{
		"connected": false,
		"disconnect_reason": gorm.Expr(
			"CASE WHEN disconnect_reason IS NULL OR disconnect_reason = '' OR disconnect_reason = ? THEN ? ELSE disconnect_reason END",
			genericReason,
			genericReason,
		),
		"disconnected_at": gorm.Expr("COALESCE(disconnected_at, ?)", time.Now().UTC()),
	}

	return i.db.Model(&instance_model.Instance{}).
		Where("id = ?", userId).
		Updates(updates).Error
}

// AppendDisconnectEvent stores one event and trims older rows for that
// instance in the same transaction. The returned error is intentionally left
// to the event caller to log and swallow so diagnostics can never interrupt a
// whatsmeow event handler.
func (i *instanceRepository) AppendDisconnectEvent(event InstanceDisconnectEvent) error {
	if event.InstanceID == "" {
		return fmt.Errorf("instance repository: disconnect event instance id is empty")
	}
	if event.EventName == "" {
		return fmt.Errorf("instance repository: disconnect event name is empty")
	}

	if event.RepeatCount < 1 {
		event.RepeatCount = 1
	}

	return i.db.Transaction(func(tx *gorm.DB) error {
		// A retry loop re-emits the same event forever. Inserting every
		// repetition pushes the rows that actually explain the incident past
		// the retention cap, so fold a consecutive duplicate into the row it
		// repeats instead of appending a new one.
		var latest InstanceDisconnectEvent
		err := tx.Where("instance_id = ?", event.InstanceID).
			Order("timestamp DESC, id DESC").
			First(&latest).Error
		switch {
		case err == nil:
			if latest.EventName == event.EventName &&
				latest.Reason == event.Reason &&
				latest.Jid == event.Jid {
				return tx.Model(&InstanceDisconnectEvent{}).
					Where("id = ?", latest.Id).
					Updates(map[string]interface{}{
						"timestamp":    event.Timestamp,
						"repeat_count": gorm.Expr("repeat_count + ?", event.RepeatCount),
					}).Error
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			// First event for this instance; fall through to the insert.
		default:
			return err
		}

		if err := tx.Create(&event).Error; err != nil {
			return err
		}

		recentEvents := tx.Model(&InstanceDisconnectEvent{}).
			Select("id").
			Where("instance_id = ?", event.InstanceID).
			Order("timestamp DESC, id DESC").
			Limit(maxDisconnectEventsPerInstance)

		return tx.Where("instance_id = ? AND id NOT IN (?)", event.InstanceID, recentEvents).
			Delete(&InstanceDisconnectEvent{}).Error
	})
}

func (i *instanceRepository) InitializeDisconnectedTracking(now time.Time, clientName string) (int64, error) {
	query := i.db.Model(&instance_model.Instance{}).
		Where("connected = ? AND disconnected_at IS NULL", false)
	if clientName != "" {
		query = query.Where("client_name = ?", clientName)
	}
	result := query.Update("disconnected_at", now.UTC())
	return result.RowsAffected, result.Error
}

func (i *instanceRepository) GetDisconnectedBefore(cutoff time.Time, clientName string) ([]*instance_model.Instance, error) {
	var instances []*instance_model.Instance
	query := i.db.
		Where("connected = ? AND disconnected_at IS NOT NULL AND disconnected_at <= ?", false, cutoff.UTC()).
		Order("disconnected_at ASC")
	if clientName != "" {
		query = query.Where("client_name = ?", clientName)
	}
	if err := query.Find(&instances).Error; err != nil {
		return nil, err
	}
	return instances, nil
}

func (i *instanceRepository) UpdateQrcode(userId string, qr string) error {
	return i.db.Model(&instance_model.Instance{}).Where("id = ?", userId).Update("qrcode", qr).Error
}

func (i *instanceRepository) UpdateProxy(userId string, proxy string) error {
	return i.db.Model(&instance_model.Instance{}).Where("id = ?", userId).Update("proxy", proxy).Error
}

func (i *instanceRepository) UpdateJid(userId string, jid string) error {
	return i.db.Model(&instance_model.Instance{}).Where("id = ?", userId).Update("jid", jid).Error
}

func (i *instanceRepository) GetAllConnectedInstances() ([]*instance_model.Instance, error) {
	var instances []*instance_model.Instance
	err := i.db.Where("connected = ?", true).Find(&instances).Error
	if err != nil {
		return nil, err
	}

	return instances, nil
}

func (i *instanceRepository) GetAllConnectedInstancesByClientName(clientName string) ([]*instance_model.Instance, error) {
	var instances []*instance_model.Instance
	err := i.db.Where("connected = ? AND client_name = ?", true, clientName).Find(&instances).Error
	if err != nil {
		return nil, err
	}

	return instances, nil
}

func (i *instanceRepository) GetAll(clientName string) ([]*instance_model.Instance, error) {
	var instances []*instance_model.Instance
	err := i.db.Where("client_name = ?", clientName).Find(&instances).Error
	if err != nil {
		return nil, err
	}

	return instances, nil
}

func (i *instanceRepository) Delete(instanceId string) error {
	return i.db.Transaction(func(tx *gorm.DB) error {
		// Deleta todas as labels associadas à instância
		if err := tx.Where("instance_id = ?", instanceId).Delete(&label_model.Label{}).Error; err != nil {
			return fmt.Errorf("erro ao deletar labels: %v", err)
		}

		// Deleta todas as mensagens associadas à instância
		if err := tx.Where("source = ?", instanceId).Delete(&message_model.Message{}).Error; err != nil {
			return fmt.Errorf("erro ao deletar mensagens: %v", err)
		}

		// Deleta a instância
		if err := tx.Where("id = ?", instanceId).Delete(&instance_model.Instance{}).Error; err != nil {
			return fmt.Errorf("erro ao deletar instância: %v", err)
		}

		return nil
	})
}

func (i *instanceRepository) GetAdvancedSettings(instanceId string) (*instance_model.AdvancedSettings, error) {
	// Valida o formato do UUID
	if _, err := uuid.Parse(instanceId); err != nil {
		return nil, fmt.Errorf("invalid UUID format: %v", err)
	}

	var instance instance_model.Instance
	err := i.db.Select("always_online, reject_call, msg_reject_call, read_messages, ignore_groups, ignore_status").
		Where("id = ?", instanceId).First(&instance).Error
	if err != nil {
		return nil, err
	}

	settings := &instance_model.AdvancedSettings{
		AlwaysOnline:  instance.AlwaysOnline,
		RejectCall:    instance.RejectCall,
		MsgRejectCall: instance.MsgRejectCall,
		ReadMessages:  instance.ReadMessages,
		IgnoreGroups:  instance.IgnoreGroups,
		IgnoreStatus:  instance.IgnoreStatus,
	}

	return settings, nil
}

func (i *instanceRepository) UpdateAdvancedSettings(instanceId string, settings *instance_model.AdvancedSettings) error {
	// Valida o formato do UUID
	if _, err := uuid.Parse(instanceId); err != nil {
		return fmt.Errorf("invalid UUID format: %v", err)
	}

	updates := map[string]interface{}{
		"always_online":   settings.AlwaysOnline,
		"reject_call":     settings.RejectCall,
		"msg_reject_call": settings.MsgRejectCall,
		"read_messages":   settings.ReadMessages,
		"ignore_groups":   settings.IgnoreGroups,
		"ignore_status":   settings.IgnoreStatus,
	}

	err := i.db.Model(&instance_model.Instance{}).Where("id = ?", instanceId).Updates(updates).Error
	if err != nil {
		logger.LogError("Error updating advanced settings in DB: %v", err)
		return err
	}

	return nil
}

func NewInstanceRepository(db *gorm.DB) InstanceRepository {
	return &instanceRepository{
		db: db,
	}
}
