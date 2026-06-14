package message_service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/EvolutionAPI/evolution-go/pkg/logger"
	message_model "github.com/EvolutionAPI/evolution-go/pkg/message/model"
	message_repository "github.com/EvolutionAPI/evolution-go/pkg/message/repository"
	"github.com/EvolutionAPI/evolution-go/pkg/utils"
	whatsmeow_service "github.com/EvolutionAPI/evolution-go/pkg/whatsmeow/service"
	"github.com/vincent-petithory/dataurl"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type MessageService interface {
	React(data *ReactStruct, instance *instance_model.Instance) (*MessageSendStruct, error)
	ChatPresence(data *ChatPresenceStruct, instance *instance_model.Instance) (string, error)
	MarkRead(data *MarkReadStruct, instance *instance_model.Instance) (string, error)
	DownloadMedia(data *DownloadMediaStruct, instance *instance_model.Instance, request *http.Request) (*dataurl.DataURL, string, error)
	GetMessageStatus(data *MessageStatusStruct, instance *instance_model.Instance) (*message_model.Message, string, error)
	DeleteMessageEveryone(data *MessageStruct, instance *instance_model.Instance) (string, string, error)
	EditMessage(data *EditMessageStruct, instance *instance_model.Instance) (string, string, error)
	SubscribePresence(data *SubscribePresenceStruct, instance *instance_model.Instance) error
	SetSelfPresence(available bool, instance *instance_model.Instance) error
	VotePoll(data *VotePollStruct, instance *instance_model.Instance) (string, error)
}

type messageService struct {
	clientPointer     map[string]*whatsmeow.Client
	messageRepository message_repository.MessageRepository
	whatsmeowService  whatsmeow_service.WhatsmeowService
	loggerWrapper     *logger_wrapper.LoggerManager
}

type ReactStruct struct {
	Number      string `json:"number"`
	Reaction    string `json:"reaction"`
	Id          string `json:"id"`
	FromMe      bool   `json:"fromMe"`
	Participant string `json:"participant,omitempty"`
}

type ChatPresenceStruct struct {
	Number  string `json:"number"`
	State   string `json:"state"`
	IsAudio bool   `json:"isAudio"`
}

type SubscribePresenceStruct struct {
	Number string `json:"number"`
}

type SelfPresenceStruct struct {
	Available bool `json:"available"`
}

type VotePollStruct struct {
	Number        string   `json:"number"` // deliverable chat JID (where to SEND the vote)
	PollMessageID string   `json:"pollMessageId"`
	Options       []string `json:"options"`
	PollChat      string   `json:"pollChat"` // pristine chat JID of the poll (for key/secret)
	Sender        string   `json:"sender"`   // pristine creator JID (for the message secret)
	FromMe        bool     `json:"fromMe"`   // true if WE created the poll
}

type MarkReadStruct struct {
	Id     []string `json:"id"`
	Number string   `json:"number"`
}

type DownloadMediaStruct struct {
	Message *waE2E.Message `json:"message"`
}

type MessageStatusStruct struct {
	Id string `json:"id"`
}

type MessageStruct struct {
	Chat      string `json:"chat"`
	MessageID string `json:"messageId"`
}

type EditMessageStruct struct {
	Chat      string `json:"chat"`
	Message   string `json:"message"`
	MessageID string `json:"messageId"`
}

type MessageSendStruct struct {
	Info               types.MessageInfo
	Message            *waE2E.Message
	MessageContextInfo *waE2E.ContextInfo
}

func (m *messageService) ensureClientConnected(instanceId string) (*whatsmeow.Client, error) {
	client := m.clientPointer[instanceId]
	m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking client connection status - Client exists: %v", instanceId, client != nil)

	if client == nil {
		m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] No client found, attempting to start new instance", instanceId)
		err := m.whatsmeowService.StartInstance(instanceId)
		if err != nil {
			m.loggerWrapper.GetLogger(instanceId).LogError("[%s] Failed to start instance: %v", instanceId, err)
			return nil, errors.New("no active session found")
		}

		m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Instance started, waiting 2 seconds...", instanceId)
		time.Sleep(2 * time.Second)

		client = m.clientPointer[instanceId]
		m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking new client - Exists: %v, Connected: %v",
			instanceId,
			client != nil,
			client != nil && client.IsConnected())

		if client == nil || !client.IsConnected() {
			m.loggerWrapper.GetLogger(instanceId).LogError("[%s] New client validation failed - Exists: %v, Connected: %v",
				instanceId,
				client != nil,
				client != nil && client.IsConnected())
			return nil, errors.New("no active session found")
		}
	} else if !client.IsConnected() {
		m.loggerWrapper.GetLogger(instanceId).LogError("[%s] Existing client is disconnected - Connected status: %v",
			instanceId,
			client.IsConnected())
		return nil, errors.New("client disconnected")
	}

	m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Client successfully validated - Connected: %v", instanceId, client.IsConnected())
	return client, nil
}

func (m *messageService) React(data *ReactStruct, instance *instance_model.Instance) (*MessageSendStruct, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, err
	}

	msgId := ""

	recipient, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return nil, errors.New("invalid phone number")
	}

	if data.Id == "" {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Missing Id in Payload", instance.Id)
		return nil, errors.New("missing id in payload")
	} else {
		msgId = data.Id
	}

	fromMe := data.FromMe
	reaction := data.Reaction
	if reaction == "remove" {
		reaction = ""
	}

	// Create MessageKey
	messageKey := &waCommon.MessageKey{
		RemoteJID: proto.String(recipient.String()),
		FromMe:    proto.Bool(fromMe),
		ID:        proto.String(msgId),
	}

	// Add participant if provided (for group messages)
	if data.Participant != "" {
		participantJID, ok := utils.ParseJID(data.Participant)
		if ok {
			messageKey.Participant = proto.String(participantJID.String())
		}
	}

	msg := &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Key:  messageKey,
			Text: proto.String(reaction),
			// GroupingKey:       proto.String(reaction),
			SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
		},
	}

	response, err := client.SendMessage(context.Background(), recipient, msg, whatsmeow.SendRequestExtra{
		ID: msgId,
	})
	if err != nil {
		return nil, err
	}

	isGroup := strings.Contains(data.Number, "@g.us")
	messageType := "ReactionMessage"

	messageInfo := types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     recipient,
			Sender:   *client.Store.ID,
			IsFromMe: true,
			IsGroup:  isGroup,
		},
		ID:        msgId,
		Timestamp: time.Now(),
		ServerID:  response.ServerID,
		Type:      messageType,
	}

	messageSent := &MessageSendStruct{
		Info:    messageInfo,
		Message: msg,
	}

	return messageSent, nil
}

func (m *messageService) ChatPresence(data *ChatPresenceStruct, instance *instance_model.Instance) (string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	media := ""

	if data.IsAudio {
		media = "audio"
	}

	err = client.SendChatPresence(context.Background(), recipient, types.ChatPresence(data.State), types.ChatPresenceMedia(media))
	if err != nil {
		return "", err
	}

	m.loggerWrapper.GetLogger(instance.Id).LogInfo("Message sent to %s", data.Number)

	return ts.String(), nil
}

// SubscribePresence asks the WhatsApp server to start delivering presence
// (online/offline/last-seen) and chat-presence (typing/recording) updates for a
// contact. The server only sends these to clients that are themselves marked
// available, so we send our own available presence first. Mirrors what
// WhatsApp Web does when you open a conversation.
func (m *messageService) SubscribePresence(data *SubscribePresenceStruct, instance *instance_model.Instance) error {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}

	recipient, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating presence subscribe number", instance.Id)
		return errors.New("invalid phone number")
	}

	// IMPORTANT: do NOT send available presence here. Announcing available makes
	// WhatsApp route notifications to this companion and silences the phone. The
	// caller must opt in explicitly via SetSelfPresence(true) first; this only
	// subscribes (a no-op for receiving until the client is available).
	if err := client.SubscribePresence(context.Background(), recipient); err != nil {
		return err
	}

	m.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Subscribed to presence of %s", instance.Id, data.Number)
	return nil
}

// SetSelfPresence flips our global availability. available=true lets us receive
// others' presence/typing BUT silences the phone's notifications (WhatsApp
// routes them to the active companion). available=false restores the phone.
// Presence in the web client is therefore strictly opt-in.
func (m *messageService) SetSelfPresence(available bool, instance *instance_model.Instance) error {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}
	state := types.PresenceUnavailable
	if available {
		state = types.PresenceAvailable
	}
	if err := client.SendPresence(context.Background(), state); err != nil {
		return err
	}
	m.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Self presence set to %s", instance.Id, state)
	return nil
}

// VotePoll casts/updates this account's vote on a poll. optionNames must match
// the poll's option texts exactly (whatsmeow hashes them).
func (m *messageService) VotePoll(data *VotePollStruct, instance *instance_model.Instance) (string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}
	// Delivery target: where the vote message is actually SENT (must be a
	// reachable chat JID — the phone form, not @lid).
	deliveryJID, ok := utils.ParseJID(data.Number)
	if !ok {
		return "", errors.New("invalid chat jid")
	}
	// Key/secret derivation: use the PRISTINE poll JIDs so the
	// PollCreationMessageKey matches the original (and the secret resolves).
	pollChat := deliveryJID
	if data.PollChat != "" {
		if parsed, ok2 := utils.ParseJID(data.PollChat); ok2 {
			pollChat = parsed
		}
	}
	sender := pollChat
	if data.Sender != "" {
		if parsed, ok2 := utils.ParseJID(data.Sender); ok2 {
			sender = parsed
		}
	}
	pollInfo := &types.MessageInfo{
		ID: data.PollMessageID,
		MessageSource: types.MessageSource{
			Chat:     pollChat,
			Sender:   sender,
			IsFromMe: data.FromMe,
			IsGroup:  pollChat.Server == types.GroupServer,
		},
	}
	voteMsg, err := client.BuildPollVote(context.Background(), pollInfo, data.Options)
	if err != nil {
		return "", err
	}
	resp, err := client.SendMessage(context.Background(), deliveryJID, voteMsg)
	if err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (m *messageService) MarkRead(data *MarkReadStruct, instance *instance_model.Instance) (string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	jid, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	err = client.MarkRead(context.Background(), data.Id, time.Now(), jid, jid)
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error marking message as read: %v", instance.Id, err)
		return "", errors.New("error marking message as read")
	}

	return ts.String(), nil
}

func (m *messageService) DownloadMedia(data *DownloadMediaStruct, instance *instance_model.Instance, request *http.Request) (*dataurl.DataURL, string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, "", err
	}

	var ts time.Time

	msg := data.Message

	mimetype := ""
	var mediaData []byte

	img := msg.GetImageMessage()
	audio := msg.GetAudioMessage()
	document := msg.GetDocumentMessage()
	video := msg.GetVideoMessage()
	sticker := msg.GetStickerMessage()

	if img == nil && audio == nil && document == nil && video == nil && sticker == nil {
		return nil, "", errors.New("invalid media type")
	}

	userDirectory := fmt.Sprintf(`files/user_%s`, instance.Id)
	_, err = os.Stat(userDirectory)
	if os.IsNotExist(err) {
		errDir := os.MkdirAll(userDirectory, 0751)
		if errDir != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Could not create user directory (%s)", instance.Id, userDirectory)
			return nil, "", errDir
		}
	}

	if img != nil {
		mediaData, err = client.Download(context.Background(), img)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download image", instance.Id)
			msg := fmt.Sprintf("Failed to download image %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = img.GetMimetype()
	}

	if audio != nil {
		mediaData, err = client.Download(context.Background(), audio)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download audio", instance.Id)
			msg := fmt.Sprintf("Failed to download audio %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = audio.GetMimetype()
	}

	if document != nil {
		mediaData, err = client.Download(context.Background(), document)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download document", instance.Id)
			msg := fmt.Sprintf("Failed to download document %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = document.GetMimetype()
	}

	if video != nil {
		mediaData, err = client.Download(context.Background(), video)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download video", instance.Id)
			msg := fmt.Sprintf("Failed to download video %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = video.GetMimetype()
	}

	if sticker != nil {
		mediaData, err = client.Download(context.Background(), sticker)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download sticker", instance.Id)
			msg := fmt.Sprintf("Failed to download sticker %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = sticker.GetMimetype()
	}

	dataURL := dataurl.New(mediaData, mimetype)

	return dataURL, ts.String(), nil
}

func (m *messageService) GetMessageStatus(data *MessageStatusStruct, instance *instance_model.Instance) (*message_model.Message, string, error) {
	_, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, "", err
	}

	var ts time.Time

	result, err := m.messageRepository.GetMessageByID(data.Id)
	if err != nil {
		return nil, "", err
	}

	return result, ts.String(), nil
}

func (m *messageService) DeleteMessageEveryone(data *MessageStruct, instance *instance_model.Instance) (string, string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", "", errors.New("invalid phone number")
	}

	m.loggerWrapper.GetLogger(instance.Id).LogInfo("Revoking message %s from %s", data.MessageID, recipient)

	resp, err := client.SendMessage(
		context.Background(),
		recipient,
		client.BuildRevoke(recipient, types.EmptyJID, data.MessageID))
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error revoking message: %v", instance.Id, err)
		return "", "", err
	}

	response := resp.ID

	return response, ts.String(), nil
}

func (m *messageService) EditMessage(data *EditMessageStruct, instance *instance_model.Instance) (string, string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", "", err
	}

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", "", errors.New("invalid phone number")
	}

	resp, err := client.SendMessage(
		context.Background(),
		recipient,
		client.BuildEdit(
			recipient,
			data.MessageID,
			&waE2E.Message{
				ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text: &data.Message,
				},
			}))
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error revoking message: %v", instance.Id, err)
		return "", "", err
	}

	return resp.ID, resp.Timestamp.String(), nil
}

func NewMessageService(
	clientPointer map[string]*whatsmeow.Client,
	messageRepository message_repository.MessageRepository,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	loggerWrapper *logger_wrapper.LoggerManager,
) MessageService {
	return &messageService{
		clientPointer:     clientPointer,
		messageRepository: messageRepository,
		whatsmeowService:  whatsmeowService,
		loggerWrapper:     loggerWrapper,
	}
}
