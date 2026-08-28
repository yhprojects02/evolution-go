package instance_service

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/EvolutionAPI/evolution-go/pkg/config"
	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	instance_repository "github.com/EvolutionAPI/evolution-go/pkg/instance/repository"
	event_types "github.com/EvolutionAPI/evolution-go/pkg/internal/event_types"
	logger_wrapper "github.com/EvolutionAPI/evolution-go/pkg/logger"
	"github.com/EvolutionAPI/evolution-go/pkg/registry"
	"github.com/EvolutionAPI/evolution-go/pkg/utils"
	whatsmeow_service "github.com/EvolutionAPI/evolution-go/pkg/whatsmeow/service"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type InstanceService interface {
	Create(data *CreateStruct) (*instance_model.Instance, error)
	Connect(data *ConnectStruct, instance *instance_model.Instance) (*instance_model.Instance, string, string, error)
	Reconnect(instance *instance_model.Instance) error
	Disconnect(instance *instance_model.Instance) (*instance_model.Instance, error)
	Logout(instance *instance_model.Instance) (*instance_model.Instance, error)
	Status(instance *instance_model.Instance) (*StatusStruct, error)
	GetQr(instance *instance_model.Instance) (*QrcodeStruct, error)
	Pair(data *PairStruct, instance *instance_model.Instance) (*PairReturnStruct, error)
	GetAll() ([]*instance_model.Instance, error)
	Info(instanceId string) (*instance_model.Instance, error)
	Delete(id string) error
	SetProxy(id string, proxyConfig *ProxyConfig) error
	SetProxyFromStruct(id string, data *SetProxyStruct) error
	RemoveProxy(id string) error
	ForceReconnect(instanceId string, number string) error
	GetInstanceByToken(token string) (*instance_model.Instance, error)
	GetLogs(instanceId string, startDate, endDate time.Time, level string, limit int) ([]logger_wrapper.LogEntry, error)
	GetAdvancedSettings(instanceId string) (*instance_model.AdvancedSettings, error)
	UpdateAdvancedSettings(instanceId string, settings *instance_model.AdvancedSettings) error
	CleanupExpiredDisconnected(now time.Time, retention time.Duration) (CleanupResult, error)
}

type instances struct {
	instanceRepository instance_repository.InstanceRepository
	config             *config.Config
	killChannel        *registry.KillChannels
	clientPointer      *registry.Clients
	whatsmeowService   whatsmeow_service.WhatsmeowService
	loggerWrapper      *logger_wrapper.LoggerManager
	loginLocks         *keyedMutex
}

type ProxyConfig struct {
	Protocol string `json:"protocol,omitempty"`
	Port     string `json:"port"`
	Password string `json:"password"`
	Username string `json:"username"`
	Host     string `json:"host"`
}

type CreateStruct struct {
	InstanceId       string                           `json:"instanceId"`
	Name             string                           `json:"name"`
	Token            string                           `json:"token"`
	OsName           string                           `json:"os_name"`
	Proxy            *ProxyConfig                     `json:"proxy"`
	AdvancedSettings *instance_model.AdvancedSettings `json:"advancedSettings"`
}

type ConnectStruct struct {
	WebhookUrl      string   `json:"webhookUrl"`
	Subscribe       []string `json:"subscribe"`
	Immediate       bool     `json:"immediate"`
	Phone           string   `json:"phone"`
	RabbitmqEnable  string   `json:"rabbitmqEnable"`
	WebSocketEnable string   `json:"websocketEnable"`
	NatsEnable      string   `json:"natsEnable"`
}

type StatusStruct struct {
	Connected bool
	LoggedIn  bool
	myJid     *types.JID
	Name      string
}

type QrcodeStruct struct {
	Qrcode string
	Code   string
}

type PairStruct struct {
	Subscribe []string `json:"subscribe"`
	Phone     string   `json:"phone"`
}

type PairReturnStruct struct {
	PairingCode string
}

type SetProxyStruct struct {
	Protocol string `json:"protocol,omitempty"`
	Host     string `json:"host" validate:"required"`
	Port     string `json:"port" validate:"required"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type ForceReconnectStruct struct {
	Number string `json:"number"`
}

type CleanupResult struct {
	TrackingInitialized int64
	Deleted             int
	Failed              int
}

func (i *instances) ensureClientConnected(instanceId string) (*whatsmeow.Client, error) {
	logger := i.loggerWrapper.GetLogger(instanceId)
	client := i.clientPointer.Get(instanceId)
	logger.LogInfo("[%s] Checking client connection status - Client exists: %v", instanceId, client != nil)

	if client == nil {
		logger.LogInfo("[%s] No client found, attempting to start new instance", instanceId)
		err := i.whatsmeowService.StartInstance(instanceId)
		if err != nil {
			logger.LogError("[%s] Failed to start instance: %v", instanceId, err)
			return nil, errors.New("no active session found")
		}

		logger.LogInfo("[%s] Instance started, waiting 2 seconds...", instanceId)
		time.Sleep(2 * time.Second)

		client = i.clientPointer.Get(instanceId)
		logger.LogInfo("[%s] Checking new client - Exists: %v, Connected: %v",
			instanceId,
			client != nil,
			client != nil && client.IsConnected())

		if client == nil || !client.IsConnected() {
			logger.LogError("[%s] New client validation failed - Exists: %v, Connected: %v",
				instanceId,
				client != nil,
				client != nil && client.IsConnected())
			return nil, errors.New("no active session found")
		}
	} else if !client.IsConnected() {
		logger.LogError("[%s] Existing client is disconnected - Connected status: %v",
			instanceId,
			client.IsConnected())
		return nil, errors.New("client disconnected")
	}

	logger.LogInfo("[%s] Client successfully validated - Connected: %v", instanceId, client.IsConnected())
	return client, nil
}

func (i instances) Create(data *CreateStruct) (*instance_model.Instance, error) {
	if data.Proxy != nil {
		data.Proxy.Protocol = utils.NormalizeProxyProtocol(data.Proxy.Protocol, data.Proxy.Port)
	}

	proxyJson, err := json.Marshal(data.Proxy)
	if err != nil {
		return nil, err
	}

	findInstance, _ := i.instanceRepository.GetInstanceByName(data.Name)

	if findInstance != nil {
		return nil, fmt.Errorf("instance already exists")
	}

	osName := data.OsName
	if osName == "" {
		osName = i.config.OsName
	}

	instance := instance_model.Instance{
		Id:         data.InstanceId,
		Name:       data.Name,
		Token:      data.Token,
		OsName:     osName,
		Proxy:      string(proxyJson),
		Connected:  false,
		ClientName: i.config.ClientName,
	}

	// Set advanced settings if provided
	if data.AdvancedSettings != nil {
		instance.AlwaysOnline = data.AdvancedSettings.AlwaysOnline
		instance.RejectCall = data.AdvancedSettings.RejectCall
		instance.MsgRejectCall = data.AdvancedSettings.MsgRejectCall
		instance.ReadMessages = data.AdvancedSettings.ReadMessages
		instance.IgnoreGroups = data.AdvancedSettings.IgnoreGroups
		instance.IgnoreStatus = data.AdvancedSettings.IgnoreStatus
	}

	createdInstance, err := i.instanceRepository.Create(instance)
	if err != nil {
		return nil, err
	}

	return createdInstance, nil
}

func (i instances) Connect(data *ConnectStruct, instance *instance_model.Instance) (*instance_model.Instance, string, string, error) {
	var subscribedEvents []string

	i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Processing subscribe events: %v", instance.Id, data.Subscribe)

	if len(data.Subscribe) == 0 {
		subscribedEvents = append(subscribedEvents, event_types.MESSAGE)
	} else if len(data.Subscribe) > 0 && data.Subscribe[0] == "ALL" {
		for _, event := range event_types.AllEventTypes {
			subscribedEvents = append(subscribedEvents, event)
		}
	} else {
		for _, arg := range data.Subscribe {
			if !event_types.IsEventType(arg) {
				i.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] Message type discarded '%s'", instance.Id, arg)
				continue
			}
			subscribedEvents = append(subscribedEvents, arg)
		}
	}

	eventString := strings.Join(subscribedEvents, ",")

	instance.Events = eventString
	instance.Webhook = data.WebhookUrl
	instance.RabbitmqEnable = data.RabbitmqEnable
	instance.NatsEnable = data.NatsEnable
	instance.WebSocketEnable = data.WebSocketEnable

	err := i.instanceRepository.Update(instance)
	if err != nil {
		i.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error updating instance: %s", instance.Id, err)
		return nil, "", "", err
	}

	// Verifica se a instância já está rodando
	isInstanceRunning := i.clientPointer.Get(instance.Id) != nil

	// Sincroniza as configurações na instância em execução (se já estiver conectada)
	err = i.whatsmeowService.UpdateInstanceSettings(instance.Id)
	if err != nil {
		i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Instance not in runtime yet, will be updated when connected", instance.Id)
		isInstanceRunning = false
	} else {
		i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Instance settings updated successfully in runtime", instance.Id)
		isInstanceRunning = true
	}

	// Se a instância não estiver rodando, inicia uma nova
	if !isInstanceRunning {
		i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Starting new client instance", instance.Id)

		i.killChannel.Reset(instance.Id)

		clientData := &whatsmeow_service.ClientData{
			Instance:      instance,
			Subscriptions: subscribedEvents,
			Phone:         data.Phone,
			IsProxy:       false,
			// /instance/connect is how a login begins, so this is the one path
			// allowed to register a new companion device.
			AllowNewDevice: true,
		}

		if instance.Proxy != "" || i.config.ProxyHost != "" {
			var proxyConfig ProxyConfig
			err := json.Unmarshal([]byte(instance.Proxy), &proxyConfig)
			if err != nil {
				i.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error unmarshalling proxy config: %v", instance.Id, err)
				return nil, "", "", err
			}

			if proxyConfig.Host != "" || i.config.ProxyHost != "" {
				clientData.IsProxy = true
			}
		}

		go i.whatsmeowService.StartClient(clientData)
	} else {
		i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Instance already running, settings updated without restarting client", instance.Id)
	}

	// logger.LogInfo("Waiting 1 seconds")
	// time.Sleep(1000 * time.Millisecond)

	// if i.clientPointer.Get(instance.Id) != nil {
	// 	if !i.clientPointer.Get(instance.Id).IsConnected() {
	// 		return instance, "", "", fmt.Errorf("failed to connect")
	// 	}
	// } else {
	// 	return instance, "", "", fmt.Errorf("failed to connect")
	// }

	return instance, instance.Jid, eventString, nil
}

func (i instances) Reconnect(instance *instance_model.Instance) error {
	_, err := i.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}

	return i.whatsmeowService.ReconnectClient(instance.Id)
}

func (i instances) Disconnect(instance *instance_model.Instance) (*instance_model.Instance, error) {
	client, err := i.ensureClientConnected(instance.Id)
	if err != nil {
		return instance, err
	}

	if client.IsConnected() {
		if client.IsLoggedIn() {
			i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Disconnection successful", instance.Id)
			i.killChannel.Signal(instance.Id)

			instance.Events = ""

			err := i.instanceRepository.Update(instance)
			if err != nil {
				return instance, err
			}

			return instance, nil
		}
	}

	i.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] Ignoring disconnect as it was not connected", instance.Id)
	return instance, nil
}

func (i instances) Logout(instance *instance_model.Instance) (*instance_model.Instance, error) {
	client, err := i.ensureClientConnected(instance.Id)
	if err != nil {
		return instance, err
	}

	if client.IsLoggedIn() && client.IsConnected() {
		err := client.Logout(context.Background())
		if err != nil {
			return instance, err
		}

		instance.Connected = false
		err = i.instanceRepository.Update(instance)
		if err != nil {
			return instance, err
		}

		i.killChannel.Signal(instance.Id)

		i.clientPointer.Delete(instance.Id)
		i.killChannel.Delete(instance.Id)

		i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Logout successful", instance.Id)
		return instance, nil
	}

	if client.IsConnected() {
		client.Disconnect()

		i.killChannel.Signal(instance.Id)

		i.clientPointer.Delete(instance.Id)
		i.killChannel.Delete(instance.Id)

		i.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Disconnection successful", instance.Id)
		return instance, nil
	}

	i.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] Ignoring logout as it was not connected", instance.Id)
	return instance, fmt.Errorf("ignoring logout as it was not connected")
}

// Status reports what the instance's connection is doing. It never fails
// because the connection is down: "down" is the answer, not an error.
//
// It used to call ensureClientConnected, so any instance whose socket was not
// up came back as HTTP 400 {"error":"client disconnected"} — the exact state a
// caller polls this endpoint to observe while a user is scanning a QR code.
// Every consumer treated that as fatal, stopped polling, and showed the
// operator "client disconnected" mid-link. It also made a GET start a client
// as a side effect and block for two seconds doing it.
//
// Reviving a client that should be up is still worth doing, so it is kicked
// off in the background (rate-limited, so a 2.5s poll cannot stampede) and
// this call answers immediately with the truth.
func (i instances) Status(instance *instance_model.Instance) (*StatusStruct, error) {
	client := i.clientPointer.Get(instance.Id)

	// Only an instance that finished a login (it owns a JID) is worth
	// reviving. One that never linked has nothing to reconnect to, and
	// restarting it here would start QR sessions nobody asked for.
	if (client == nil || !client.IsConnected()) && strings.TrimSpace(instance.Jid) != "" {
		i.whatsmeowService.ReviveInstance(instance.Id)
	}

	if client == nil {
		return &StatusStruct{Connected: false, LoggedIn: false}, nil
	}

	isConnected := client.IsConnected()
	isLoggedIn := client.IsLoggedIn()

	var myJid *types.JID
	var name string
	if isLoggedIn {
		myJid = client.Store.ID
		name = client.Store.PushName
	}

	status := &StatusStruct{
		Connected: isConnected,
		LoggedIn:  isLoggedIn,
		myJid:     myJid,
		Name:      name,
	}

	return status, nil
}

// qrWaitTimeout bounds how long a QR request waits for the engine to publish a
// freshly generated code. WhatsApp rotates login codes about every 20s, so a
// caller polling every few seconds must never be handed one that is older than
// this window.
const qrWaitTimeout = 12 * time.Second

// stopLoginClient tears down a login socket that is no longer usable, so the
// next attempt starts from nothing instead of inheriting a corpse.
func (i instances) stopLoginClient(instanceId string, client *whatsmeow.Client) {
	if client != nil {
		client.Disconnect()
		i.clientPointer.DeleteIf(instanceId, client)
	}
	i.killChannel.Signal(instanceId)
	i.killChannel.Delete(instanceId)
}

// startFreshLogin clears the stored QR and restarts the login socket, then
// waits for the engine to publish a NEW code.
//
// Clearing first is the whole point. The stored code is what the previous
// attempt left behind; without wiping it this function could read the old
// value straight back and call it fresh.
func (i instances) startFreshLogin(instance *instance_model.Instance) (string, error) {
	logger := i.loggerWrapper.GetLogger(instance.Id)

	if err := i.instanceRepository.UpdateQrcode(instance.Id, ""); err != nil {
		return "", fmt.Errorf("failed to clear the previous QR code: %w", err)
	}

	if err := i.whatsmeowService.StartInstanceForLogin(instance.Id); err != nil {
		logger.LogError("[%s] Failed to start instance: %v", instance.Id, err)
		return "", fmt.Errorf("failed to start instance: %w", err)
	}

	deadline := time.Now().Add(qrWaitTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)

		if client := i.clientPointer.Get(instance.Id); client != nil && client.IsLoggedIn() {
			return "", fmt.Errorf("session already logged in")
		}

		current, err := i.instanceRepository.GetInstanceByID(instance.Id)
		if err != nil {
			return "", err
		}
		if current.Qrcode != "" {
			return current.Qrcode, nil
		}
	}

	return "", fmt.Errorf("no QR code available. Please wait a moment and try again")
}

func (i instances) GetQr(instance *instance_model.Instance) (*QrcodeStruct, error) {
	// One login attempt per instance at a time. Overlapping polls would each
	// restart the socket and each kill the other's freshly issued code.
	unlock := i.loginLocks.Lock(instance.Id)
	defer unlock()

	logger := i.loggerWrapper.GetLogger(instance.Id)
	client := i.clientPointer.Get(instance.Id)

	if client != nil && client.IsLoggedIn() {
		return nil, fmt.Errorf("session already logged in")
	}

	// A client that exists but is not connected is NOT "waiting for a scan":
	// its login socket is gone, and so is the validity of whatever QR is still
	// stored against the instance. The old code took this branch and served
	// that dead code anyway, which is why the phone answered a scan with
	// "couldn't link device" and why retrying kept producing the same expired
	// image. Throw the corpse away and start a real login.
	if client != nil && !client.IsConnected() {
		logger.LogWarn("[%s] Login socket is down; discarding the stale QR and restarting", instance.Id)
		i.stopLoginClient(instance.Id, client)
		client = nil
	}

	code := ""
	if client == nil {
		logger.LogInfo("[%s] Starting a fresh login for QR code", instance.Id)
		fresh, err := i.startFreshLogin(instance)
		if err != nil {
			return nil, err
		}
		code = fresh
	} else {
		// A live login socket is already cycling codes; take the current one.
		current, err := i.instanceRepository.GetInstanceByID(instance.Id)
		if err != nil {
			return nil, err
		}
		code = current.Qrcode
		if code == "" {
			logger.LogInfo("[%s] Connected but no QR published yet; waiting", instance.Id)
			deadline := time.Now().Add(qrWaitTimeout)
			for code == "" && time.Now().Before(deadline) {
				time.Sleep(500 * time.Millisecond)
				current, err = i.instanceRepository.GetInstanceByID(instance.Id)
				if err != nil {
					return nil, err
				}
				code = current.Qrcode
			}
			if code == "" {
				return nil, fmt.Errorf("no QR code available. Please wait a moment and try again")
			}
		}
	}

	parts := strings.Split(code, "|")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid QR code format")
	}

	qr := &QrcodeStruct{
		Qrcode: parts[0],
		Code:   parts[1],
	}

	return qr, nil
}

// pairConnectTimeout bounds how long a pairing request waits for the login
// socket to come up before giving up with a real error.
const pairConnectTimeout = 15 * time.Second

// Pair asks WhatsApp for a phone-number pairing code.
//
// The old version indexed the client map with no nil check and called
// PairPhone straight on the result, so a request that arrived before the
// client goroutine had registered itself — the normal case, since
// /instance/connect starts it asynchronously — dereferenced nil and panicked
// the request. It then discarded PairPhone's error and returned
// {"PairingCode": ""} as a success, which the caller could only report as
// "pairing code expired, start again".
func (i instances) Pair(data *PairStruct, instance *instance_model.Instance) (*PairReturnStruct, error) {
	unlock := i.loginLocks.Lock(instance.Id)
	defer unlock()

	logger := i.loggerWrapper.GetLogger(instance.Id)
	client := i.clientPointer.Get(instance.Id)

	if client != nil && client.IsLoggedIn() {
		return nil, errors.New("session already logged in")
	}

	if client != nil && !client.IsConnected() {
		logger.LogWarn("[%s] Login socket is down; restarting before requesting a pairing code", instance.Id)
		i.stopLoginClient(instance.Id, client)
		client = nil
	}

	if client == nil {
		if err := i.whatsmeowService.StartInstanceForLogin(instance.Id); err != nil {
			return nil, fmt.Errorf("failed to start instance: %w", err)
		}

		deadline := time.Now().Add(pairConnectTimeout)
		for time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)
			candidate := i.clientPointer.Get(instance.Id)
			if candidate != nil && candidate.IsConnected() {
				client = candidate
				break
			}
		}
		if client == nil {
			return nil, errors.New("could not open a WhatsApp session for pairing. Please try again")
		}
	}

	code, err := client.PairPhone(context.Background(), data.Phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		logger.LogError("[%s] PairPhone failed: %v", instance.Id, err)
		return nil, fmt.Errorf("could not request a pairing code: %w", err)
	}
	if code == "" {
		logger.LogError("[%s] PairPhone returned an empty code", instance.Id)
		return nil, errors.New("WhatsApp did not return a pairing code. Please try again")
	}

	return &PairReturnStruct{PairingCode: code}, nil
}

func (i instances) GetAll() ([]*instance_model.Instance, error) {
	instances, err := i.instanceRepository.GetAll(i.config.ClientName)
	if err != nil {
		return nil, err
	}

	for _, instance := range instances {
		if client := i.clientPointer.Get(instance.Id); client != nil {
			instance.Connected = client.IsLoggedIn()
		} else {
			instance.Connected = false
		}

		instance.Proxy = ""
	}

	return instances, nil
}

func (i instances) Info(instanceId string) (*instance_model.Instance, error) {
	instance, err := i.instanceRepository.GetInstanceByID(instanceId)
	if err != nil {
		return nil, err
	}

	// Atualiza o status connected com base no estado real do cliente
	if client := i.clientPointer.Get(instance.Id); client != nil {
		instance.Connected = client.IsLoggedIn()
	} else {
		instance.Connected = false
	}

	instance.Proxy = ""

	return instance, nil
}

func (i instances) Delete(id string) error {
	instance, err := i.instanceRepository.GetInstanceByID(id)
	if err != nil {
		return err
	}

	if i.clientPointer.Get(instance.Id) != nil && i.clientPointer.Get(instance.Id).IsConnected() {
		if i.clientPointer.Get(instance.Id).IsLoggedIn() {
			i.clientPointer.Get(instance.Id).Logout(context.Background())
		}
		i.clientPointer.Get(instance.Id).Disconnect()
	}

	// Limpar todos os recursos da instância antes de deletar
	i.clientPointer.Delete(instance.Id)
	i.killChannel.Signal(instance.Id)
	i.killChannel.Delete(instance.Id)

	// Limpar cache via whatsmeow service
	err = i.whatsmeowService.ClearInstanceCache(instance.Id, instance.Token)
	if err != nil {
		i.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] Failed to clear instance cache: %v", instance.Id, err)
	}

	err = i.instanceRepository.Delete(id)
	if err != nil {
		return err
	}

	return nil
}

func (i instances) CleanupExpiredDisconnected(now time.Time, retention time.Duration) (CleanupResult, error) {
	result := CleanupResult{}
	if retention <= 0 {
		return result, fmt.Errorf("disconnected instance retention must be positive")
	}

	initialized, err := i.instanceRepository.InitializeDisconnectedTracking(now, i.config.ClientName)
	if err != nil {
		return result, fmt.Errorf("initialize disconnected tracking: %w", err)
	}
	result.TrackingInitialized = initialized

	cutoff := now.UTC().Add(-retention)
	candidates, err := i.instanceRepository.GetDisconnectedBefore(cutoff, i.config.ClientName)
	if err != nil {
		return result, fmt.Errorf("list expired disconnected instances: %w", err)
	}

	var cleanupErrors []error
	for _, candidate := range candidates {
		if candidate == nil || !isExpiredDisconnected(candidate, cutoff) {
			continue
		}

		// The persisted flag can briefly lag behind the live WhatsApp client.
		// Never delete a client that is already connected or logged in; repair
		// the stored state instead so its seven-day clock is cleared.
		if client := i.clientPointer.Get(candidate.Id); client != nil && (client.IsConnected() || client.IsLoggedIn()) {
			if err := i.instanceRepository.UpdateConnected(candidate.Id, true, ""); err != nil {
				result.Failed++
				cleanupErrors = append(cleanupErrors, fmt.Errorf("repair connected instance %s: %w", candidate.Id, err))
			}
			continue
		}

		// Re-read immediately before the destructive operation. A reconnect
		// event may have cleared disconnected_at after the candidate query.
		current, err := i.instanceRepository.GetInstanceByID(candidate.Id)
		if err != nil {
			result.Failed++
			cleanupErrors = append(cleanupErrors, fmt.Errorf("recheck instance %s: %w", candidate.Id, err))
			continue
		}
		if !isExpiredDisconnected(current, cutoff) {
			continue
		}

		if err := i.Delete(current.Id); err != nil {
			result.Failed++
			cleanupErrors = append(cleanupErrors, fmt.Errorf("delete disconnected instance %s: %w", current.Id, err))
			continue
		}
		result.Deleted++
		i.loggerWrapper.GetLogger(current.Id).LogInfo(
			"[%s] Automatically deleted after being disconnected since %s",
			current.Id,
			current.DisconnectedAt.UTC().Format(time.RFC3339),
		)
	}

	return result, errors.Join(cleanupErrors...)
}

func isExpiredDisconnected(instance *instance_model.Instance, cutoff time.Time) bool {
	return instance != nil &&
		!instance.Connected &&
		instance.DisconnectedAt != nil &&
		!instance.DisconnectedAt.After(cutoff)
}

func (i instances) SetProxy(id string, proxyConfig *ProxyConfig) error {
	instance, err := i.instanceRepository.GetInstanceByID(id)
	if err != nil {
		return err
	}

	// Validate proxy configuration
	if proxyConfig == nil {
		return fmt.Errorf("proxy configuration cannot be nil")
	}

	if proxyConfig.Host == "" {
		return fmt.Errorf("proxy host is required")
	}

	if proxyConfig.Port == "" {
		return fmt.Errorf("proxy port is required")
	}

	proxyConfig.Protocol = utils.NormalizeProxyProtocol(proxyConfig.Protocol, proxyConfig.Port)

	// Convert proxy config to JSON
	proxyJSON, err := json.Marshal(proxyConfig)
	if err != nil {
		i.loggerWrapper.GetLogger(id).LogError("[%s] Failed to marshal proxy config: %v", id, err)
		return fmt.Errorf("failed to marshal proxy configuration: %v", err)
	}

	instance.Proxy = string(proxyJSON)

	// Update instance in database
	err = i.instanceRepository.Update(instance)
	if err != nil {
		i.loggerWrapper.GetLogger(id).LogError("[%s] Failed to update instance with proxy: %v", id, err)
		return err
	}

	i.loggerWrapper.GetLogger(id).LogInfo("[%s] Proxy configuration updated: %s://%s:%s", id, proxyConfig.Protocol, proxyConfig.Host, proxyConfig.Port)

	// Reconnect to apply proxy changes
	go i.Reconnect(instance)

	return nil
}

func (i instances) SetProxyFromStruct(id string, data *SetProxyStruct) error {
	if data == nil {
		return fmt.Errorf("proxy data cannot be nil")
	}

	proxyConfig := &ProxyConfig{
		Protocol: data.Protocol,
		Host:     data.Host,
		Port:     data.Port,
		Username: data.Username,
		Password: data.Password,
	}

	return i.SetProxy(id, proxyConfig)
}

func (i instances) RemoveProxy(id string) error {
	instance, err := i.instanceRepository.GetInstanceByID(id)
	if err != nil {
		return err
	}

	instance.Proxy = ""

	err = i.instanceRepository.Update(instance)
	if err != nil {
		return err
	}

	i.loggerWrapper.GetLogger(id).LogInfo("[%s] Proxy configuration removed", id)

	go i.Reconnect(instance)

	return nil
}

func (i instances) ForceReconnect(instanceId string, number string) error {
	// An instance with no client is exactly what this endpoint exists to fix,
	// so the nil case must fall through rather than dereference nil.
	if existing := i.clientPointer.Get(instanceId); existing != nil && existing.IsConnected() && existing.IsLoggedIn() {
		return fmt.Errorf("client already connected")
	}

	err := i.whatsmeowService.ForceUpdateJid(instanceId, number)
	if err != nil {
		return err
	}

	instance, err := i.instanceRepository.GetInstanceByID(instanceId)
	if err != nil {
		return err
	}

	subscribedEvents := strings.Split(instance.Events, ",")

	clientData := &whatsmeow_service.ClientData{
		Instance:      instance,
		Subscriptions: subscribedEvents,
		Phone:         "",
		IsProxy:       false,
	}

	if instance.Proxy != "" || i.config.ProxyHost != "" {
		var proxyConfig ProxyConfig
		err := json.Unmarshal([]byte(instance.Proxy), &proxyConfig)
		if err != nil {
			i.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error unmarshalling proxy config: %v", instance.Id, err)
			return err
		}

		if proxyConfig.Host != "" || i.config.ProxyHost != "" {
			clientData.IsProxy = true
		}
	}

	if client := i.clientPointer.Get(instance.Id); client != nil {
		client.Disconnect()

		i.killChannel.Signal(instance.Id)

		i.clientPointer.Delete(instance.Id)
		i.killChannel.Delete(instance.Id)
	}

	// Install the new generation's stop signal only AFTER the old one has been
	// signalled and dropped. Doing it first (as before) meant the teardown
	// immediately stopped the channel the incoming client was about to adopt.
	i.killChannel.Reset(instance.Id)

	go i.whatsmeowService.StartClient(clientData)

	time.Sleep(2 * time.Second)

	if i.clientPointer.Get(instance.Id) != nil {
		if !i.clientPointer.Get(instance.Id).IsConnected() {
			return fmt.Errorf("failed to connect")
		}

		if !i.clientPointer.Get(instance.Id).IsLoggedIn() {
			return fmt.Errorf("failed to login")
		}
	} else {
		return fmt.Errorf("failed to connect")
	}

	return nil
}

func (i instances) GetInstanceByToken(token string) (*instance_model.Instance, error) {
	return i.instanceRepository.GetInstanceByToken(token)
}

func (i instances) GetLogs(instanceId string, startDate, endDate time.Time, level string, limit int) ([]logger_wrapper.LogEntry, error) {
	// Inicializa o slice vazio para garantir que nunca retorne null
	logs := make([]logger_wrapper.LogEntry, 0)

	// Define valores padrão
	if limit <= 0 {
		limit = 100 // Limite padrão de 100 registros
	}

	// Se não foi fornecida data inicial, usa 7 dias atrás
	if startDate.IsZero() {
		startDate = time.Now().AddDate(0, 0, -7)
	}

	// Se não foi fornecida data final, usa data atual
	if endDate.IsZero() {
		endDate = time.Now()
	}

	// Ajusta as datas para início e fim do dia
	startDate = time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.UTC)
	endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 23, 59, 59, 999999999, time.UTC)

	// Garante que a data inicial não seja posterior à data final
	if startDate.After(endDate) {
		return logs, fmt.Errorf("data inicial não pode ser posterior à data final")
	}

	// Níveis de log válidos
	validLevels := map[string]bool{
		"INFO":  true,
		"ERROR": true,
		"WARN":  true,
		"DEBUG": true,
	}

	var levelArray []string
	if level == "" {
		// Se nenhum nível foi especificado, usa todos
		levelArray = []string{"INFO", "ERROR", "WARN", "DEBUG"}
	} else {
		// Divide e normaliza os níveis fornecidos
		for _, l := range strings.Split(level, ",") {
			l = strings.TrimSpace(strings.ToUpper(l))
			if !validLevels[l] {
				return logs, fmt.Errorf("nível de log inválido: %s", l)
			}
			levelArray = append(levelArray, l)
		}
	}

	// Lê os logs do arquivo
	logPath := filepath.Join(i.config.LogDirectory, instanceId, "instance.log")
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return logs, nil // Retorna array vazio se arquivo não existir
		}
		return logs, fmt.Errorf("erro ao abrir arquivo de log: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Aumenta o buffer do scanner para lidar com linhas grandes
	const maxCapacity = 1024 * 1024 // 1MB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		var entry logger_wrapper.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue // Ignora linhas inválidas
		}

		// Ajusta o timestamp da entrada para UTC para comparação correta
		entry.Timestamp = entry.Timestamp.UTC()

		// Aplica os filtros
		if entry.Timestamp.Before(startDate) || entry.Timestamp.After(endDate) {
			continue
		}

		if !slices.Contains(levelArray, entry.Level) {
			continue
		}

		logs = append(logs, entry)

		// Verifica o limite
		if len(logs) >= limit {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return logs, fmt.Errorf("erro ao ler arquivo de log: %v", err)
	}

	// Ordena os logs por timestamp em ordem decrescente
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].Timestamp.After(logs[j].Timestamp)
	})

	return logs, nil
}

func (i instances) GetAdvancedSettings(instanceId string) (*instance_model.AdvancedSettings, error) {
	i.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Getting advanced settings", instanceId)

	settings, err := i.instanceRepository.GetAdvancedSettings(instanceId)
	if err != nil {
		i.loggerWrapper.GetLogger(instanceId).LogError("[%s] Error getting advanced settings: %v", instanceId, err)
		return nil, err
	}

	return settings, nil
}

func (i instances) UpdateAdvancedSettings(instanceId string, settings *instance_model.AdvancedSettings) error {
	i.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Updating advanced settings", instanceId)

	err := i.instanceRepository.UpdateAdvancedSettings(instanceId, settings)
	if err != nil {
		i.loggerWrapper.GetLogger(instanceId).LogError("[%s] Error updating advanced settings: %v", instanceId, err)
		return err
	}

	// Sincroniza as configurações na instância em execução
	err = i.whatsmeowService.UpdateInstanceAdvancedSettings(instanceId)
	if err != nil {
		i.loggerWrapper.GetLogger(instanceId).LogWarn("[%s] Error syncing advanced settings to runtime: %v", instanceId, err)
		// Não falha a operação, apenas loga o warning
	}

	i.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Advanced settings updated successfully", instanceId)
	return nil
}

func NewInstanceService(
	instanceRepository instance_repository.InstanceRepository,
	killChannel *registry.KillChannels,
	clientPointer *registry.Clients,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	config *config.Config,
	loggerWrapper *logger_wrapper.LoggerManager,
) InstanceService {
	return &instances{
		instanceRepository: instanceRepository,
		killChannel:        killChannel,
		clientPointer:      clientPointer,
		whatsmeowService:   whatsmeowService,
		config:             config,
		loggerWrapper:      loggerWrapper,
		loginLocks:         newKeyedMutex(),
	}
}
