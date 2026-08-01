package config

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gomessguii/logger"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	config_env "github.com/EvolutionAPI/evolution-go/pkg/config/env"
)

type Config struct {
	PostgresAuthDB       string
	postgresUsersDB      string
	PostgresHost         string
	PostgresPort         string
	PostgresUser         string
	PostgresPassword     string
	PostgresDB           string
	DatabaseSaveMessages bool
	GlobalApiKey         string
	WaDebug              string
	LogType              string
	WebhookFiles         bool
	ConnectOnStartup     bool
	OsName               string
	AmqpUrl              string
	AmqpGlobalEnabled    bool
	WebhookUrl           string
	ClientName           string
	ApiAudioConverter    string
	ApiAudioConverterKey string
	MinioEndpoint        string
	MinioAccessKey       string
	MinioSecretKey       string
	MinioBucket          string
	MinioUseSSL          bool
	MinioEnabled         bool
	MinioRegion          string
	WhatsappVersionMajor int
	WhatsappVersionMinor int
	WhatsappVersionPatch int
	ProxyProtocol        string
	ProxyHost            string
	ProxyPort            string
	ProxyUsername        string
	ProxyPassword        string
	AmqpGlobalEvents     []string
	AmqpSpecificEvents   []string
	NatsUrl              string
	NatsGlobalEnabled    bool
	NatsGlobalEvents     []string
	EventIgnoreGroup     bool
	EventIgnoreStatus    bool
	QrcodeMaxCount       int
	CheckUserExists      bool

	// Logger configurations
	LogMaxSize    int
	LogMaxBackups int
	LogMaxAge     int
	LogDirectory  string
	LogCompress   bool

}

// EnsureDBExists connects to postgres (without the target database) and creates it if it doesn't exist.
func (c *Config) EnsureDBExists(dsn string) error {
	return ensureDBExists(dsn)
}

// ensureDBExists connects to postgres (without the target database) and creates it if it doesn't exist.
func ensureDBExists(dsn string) error {
	dbName, adminDSN, err := extractDBNameAndAdminDSN(dsn)
	if err != nil {
		return err
	}

	db, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres for auto-setup: %v", err)
	}
	defer db.Close()

	var exists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %v", err)
	}

	if !exists {
		logger.LogInfo("[CONFIG] Database %q not found, creating it automatically...", dbName)
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %q", dbName))
		if err != nil {
			return fmt.Errorf("failed to create database %q: %v", dbName, err)
		}
		logger.LogInfo("[CONFIG] Database %q created successfully", dbName)
	}

	return nil
}

// extractDBNameAndAdminDSN parses a DSN (URL or key=value) and returns the database name
// and a DSN pointing to the "postgres" maintenance database.
func extractDBNameAndAdminDSN(dsn string) (string, string, error) {
	// Try URL format: postgres://user:pass@host:port/dbname?...
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", "", fmt.Errorf("failed to parse DSN URL: %v", err)
		}
		dbName := strings.TrimPrefix(u.Path, "/")
		u.Path = "/postgres"
		return dbName, u.String(), nil
	}

	// Key=value format: host=... user=... password=... dbname=... sslmode=...
	parts := strings.Fields(dsn)
	kvMap := make(map[string]string, len(parts))
	for _, p := range parts {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			kvMap[kv[0]] = kv[1]
		}
	}
	dbName, ok := kvMap["dbname"]
	if !ok || dbName == "" {
		return "", "", fmt.Errorf("could not extract dbname from DSN")
	}
	kvMap["dbname"] = "postgres"
	adminParts := make([]string, 0, len(kvMap))
	for k, v := range kvMap {
		adminParts = append(adminParts, k+"="+v)
	}
	return dbName, strings.Join(adminParts, " "), nil
}

func (c *Config) CreateUsersDB() (*gorm.DB, error) {
	logger.LogDebug("Connecting to database on: %s", c.postgresUsersDB)

	dbDSN := c.postgresUsersDB

	if c.postgresUsersDB == "" {
		dbDSN = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", c.PostgresHost, c.PostgresPort, c.PostgresUser, c.PostgresPassword, c.PostgresDB)
	}

	if err := ensureDBExists(dbDSN); err != nil {
		logger.LogWarn("[CONFIG] Auto-setup failed (will try connecting anyway): %v", err)
	}

	// PreferSimpleProtocol stops pgx from using implicit prepared statements.
	// Supabase's transaction pooler (port 6543) hands a different backend to
	// each transaction, so a cached statement name is repeatedly re-declared and
	// the query dies with `prepared statement "stmtcache_..." already exists`
	// (SQLSTATE 42P05). That surfaced as an unreadable `instances` table: the
	// token lookup returned zero rows and the engine answered every send with
	// `401 not authorized` while the token was in fact valid.
	//
	// Simple protocol works on the session pooler too, so this holds whichever
	// port the deployment env happens to carry.
	db, err := gorm.Open(
		postgres.New(postgres.Config{
			DSN:                  dbDSN,
			PreferSimpleProtocol: true,
		}),
		&gorm.Config{},
	)
	if err != nil {
		return nil, err
	}

	// Configurar pool de conexões no GORM
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter sql.DB do GORM: %v", err)
	}

	// Supabase is a shared managed PostgreSQL service. Keep the two Evolution Go
	// pools deliberately small so Auth + user persistence cannot exhaust the
	// project's available database connections.
	sqlDB.SetMaxOpenConns(5)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxLifetime(5 * time.Minute) // Reconectar após 5 minutos para evitar timeouts
	sqlDB.SetConnMaxIdleTime(1 * time.Minute) // Fechar conexões ociosas após 1 minuto

	return db, nil
}

func (c *Config) CreateAuthDB() (*sql.DB, error) {
	dbDSN := c.PostgresAuthDB

	// Fall back on the field actually being used: keying this off
	// postgresUsersDB meant an empty auth DSN was passed through whenever the
	// users DSN happened to be set.
	if c.PostgresAuthDB == "" {
		dbDSN = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", c.PostgresHost, c.PostgresPort, c.PostgresUser, c.PostgresPassword, c.PostgresDB)
	}

	if err := ensureDBExists(dbDSN); err != nil {
		logger.LogWarn("[CONFIG] Auto-setup failed (will try connecting anyway): %v", err)
	}

	db, err := sql.Open("postgres", dbDSN)
	if err != nil {
		return nil, err
	}

	// See CreateUsersDB: this process can open both Auth and user pools.
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute) // Reconectar após 5 minutos para evitar timeouts
	db.SetConnMaxIdleTime(1 * time.Minute) // Fechar conexões ociosas após 1 minuto

	// Testar a conexão
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("erro ao testar conexão PostgreSQL AUTH: %v", err)
	}

	return db, nil
}

func Load() *Config {
	postgresAuthDB := os.Getenv(config_env.POSTGRES_AUTH_DB)

	postgresUsersDB := os.Getenv(config_env.POSTGRES_USERS_DB)

	postgresHost := os.Getenv(config_env.POSTGRES_HOST)
	postgresPort := os.Getenv(config_env.POSTGRES_PORT)
	postgresUser := os.Getenv(config_env.POSTGRES_USER)
	postgresPassword := os.Getenv(config_env.POSTGRES_PASSWORD)
	postgresDB := os.Getenv(config_env.POSTGRES_DB)

	if postgresUsersDB == "" && (postgresHost == "" || postgresPort == "" || postgresUser == "" || postgresPassword == "" || postgresDB == "") {
		logger.LogFatal("[CONFIG] required database configuration variables are missing. Please check your environment configuration.")
	}

	databaseSaveMessages := os.Getenv(config_env.DATABASE_SAVE_MESSAGES)
	panicIfEmpty(config_env.DATABASE_SAVE_MESSAGES, databaseSaveMessages)

	globalApiKey := os.Getenv(config_env.GLOBAL_API_KEY)
	panicIfEmpty(config_env.GLOBAL_API_KEY, globalApiKey)

	clientName := os.Getenv(config_env.CLIENT_NAME)

	waDebug := os.Getenv(config_env.WA_DEBUG)

	logType := os.Getenv(config_env.LOGTYPE)

	webhookFiles := os.Getenv(config_env.WEBHOOKFILES)
	if webhookFiles == "" {
		webhookFiles = "true"
	}

	connectOnStartup := os.Getenv(config_env.CONNECT_ON_STARTUP)
	if connectOnStartup == "" {
		connectOnStartup = "false"
	}

	osName := os.Getenv(config_env.OS_NAME)

	amqpUrl := os.Getenv(config_env.AMQP_URL)

	// Validate AMQP URL format
	if err := validateAMQPURL(amqpUrl); err != nil {
		logger.LogFatal("[CONFIG] AMQP URL validation failed: %v", err)
	}

	amqpGlobalEnabled := os.Getenv(config_env.AMQP_GLOBAL_ENABLED)

	webhookUrl := os.Getenv(config_env.WEBHOOK_URL)

	apiAudioConverter := os.Getenv(config_env.API_AUDIO_CONVERTER)
	apiAudioConverterKey := os.Getenv(config_env.API_AUDIO_CONVERTER_KEY)

	whatsappVersionMajor := os.Getenv(config_env.WHATSAPP_VERSION_MAJOR)
	whatsappVersionMinor := os.Getenv(config_env.WHATSAPP_VERSION_MINOR)
	whatsappVersionPatch := os.Getenv(config_env.WHATSAPP_VERSION_PATCH)

	proxyProtocol := os.Getenv(config_env.PROXY_PROTOCOL)
	proxyHost := os.Getenv(config_env.PROXY_HOST)
	proxyPort := os.Getenv(config_env.PROXY_PORT)
	proxyUsername := os.Getenv(config_env.PROXY_USERNAME)
	proxyPassword := os.Getenv(config_env.PROXY_PASSWORD)

	eventIgnoreGroup := os.Getenv(config_env.EVENT_IGNORE_GROUP)
	eventIgnoreStatus := os.Getenv(config_env.EVENT_IGNORE_STATUS)
	qrcodeMaxCount := os.Getenv(config_env.QRCODE_MAX_COUNT)
	checkUserExists := os.Getenv(config_env.CHECK_USER_EXISTS)

	if checkUserExists == "" {
		checkUserExists = "true"
	}

	// Convertendo para int com valores padrão caso estejam vazios
	major := 0
	if whatsappVersionMajor != "" {
		major, _ = strconv.Atoi(whatsappVersionMajor)
	}
	minor := 0
	if whatsappVersionMinor != "" {
		minor, _ = strconv.Atoi(whatsappVersionMinor)
	}
	patch := 0
	if whatsappVersionPatch != "" {
		patch, _ = strconv.Atoi(whatsappVersionPatch)
	}

	qrMaxCount := 5 // Valor padrão
	if qrcodeMaxCount != "" {
		qrMaxCount, _ = strconv.Atoi(qrcodeMaxCount)
	}

	amqpGlobalEvents := strings.Split(os.Getenv(config_env.AMQP_GLOBAL_EVENTS), ",")
	if len(amqpGlobalEvents) == 1 && amqpGlobalEvents[0] == "" {
		amqpGlobalEvents = []string{}
	}

	amqpSpecificEvents := strings.Split(os.Getenv(config_env.AMQP_SPECIFIC_EVENTS), ",")
	if len(amqpSpecificEvents) == 1 && amqpSpecificEvents[0] == "" {
		amqpSpecificEvents = []string{}
	}

	natsUrl := os.Getenv(config_env.NATS_URL)
	natsGlobalEnabled := os.Getenv(config_env.NATS_GLOBAL_ENABLED)
	natsGlobalEvents := strings.Split(os.Getenv(config_env.NATS_GLOBAL_EVENTS), ",")
	if len(natsGlobalEvents) == 1 && natsGlobalEvents[0] == "" {
		natsGlobalEvents = []string{}
	}

	// Logger configurations
	logMaxSize, _ := strconv.Atoi(os.Getenv(config_env.LOG_MAX_SIZE))
	if logMaxSize == 0 {
		logMaxSize = 100 // Default 100MB
	}

	logMaxBackups, _ := strconv.Atoi(os.Getenv(config_env.LOG_MAX_BACKUPS))
	if logMaxBackups == 0 {
		logMaxBackups = 5 // Default 5 backups
	}

	logMaxAge, _ := strconv.Atoi(os.Getenv(config_env.LOG_MAX_AGE))
	if logMaxAge == 0 {
		logMaxAge = 30 // Default 30 days
	}

	logDirectory := os.Getenv(config_env.LOG_DIRECTORY)
	if logDirectory == "" {
		logDirectory = "./logs" // Default logs directory
	}

	logCompress := os.Getenv(config_env.LOG_COMPRESS) == "true"
	if os.Getenv(config_env.LOG_COMPRESS) == "" {
		logCompress = true // Default compression enabled
	}

	config := &Config{
		PostgresAuthDB:       postgresAuthDB,
		postgresUsersDB:      postgresUsersDB,
		DatabaseSaveMessages: databaseSaveMessages == "true",
		GlobalApiKey:         globalApiKey,
		WaDebug:              waDebug,
		LogType:              logType,
		WebhookFiles:         webhookFiles == "true",
		ConnectOnStartup:     connectOnStartup == "true",
		OsName:               osName,
		AmqpUrl:              amqpUrl,
		AmqpGlobalEnabled:    amqpGlobalEnabled == "true",
		WebhookUrl:           webhookUrl,
		ClientName:           clientName,
		ApiAudioConverter:    apiAudioConverter,
		ApiAudioConverterKey: apiAudioConverterKey,
		PostgresHost:         postgresHost,
		PostgresPort:         postgresPort,
		PostgresUser:         postgresUser,
		PostgresPassword:     postgresPassword,
		PostgresDB:           postgresDB,
		WhatsappVersionMajor: major,
		WhatsappVersionMinor: minor,
		WhatsappVersionPatch: patch,
		ProxyProtocol:        proxyProtocol,
		ProxyHost:            proxyHost,
		ProxyPort:            proxyPort,
		ProxyUsername:        proxyUsername,
		ProxyPassword:        proxyPassword,
		EventIgnoreGroup:     eventIgnoreGroup == "true",
		EventIgnoreStatus:    eventIgnoreStatus == "true",
		QrcodeMaxCount:       qrMaxCount,
		CheckUserExists:      checkUserExists != "false", // Default true, set to false to disable
		AmqpGlobalEvents:     amqpGlobalEvents,
		AmqpSpecificEvents:   amqpSpecificEvents,
		NatsUrl:              natsUrl,
		NatsGlobalEnabled:    natsGlobalEnabled == "true",
		NatsGlobalEvents:     natsGlobalEvents,
		LogMaxSize:           logMaxSize,
		LogMaxBackups:        logMaxBackups,
		LogMaxAge:            logMaxAge,
		LogDirectory:         logDirectory,
		LogCompress:          logCompress,
	}

	minioEnabled := os.Getenv(config_env.MINIO_ENABLED) == "true"
	if minioEnabled {
		config.MinioEnabled = true
		loadMinioConfig(config)
	}

	warnIfTransactionPooler(config_env.POSTGRES_AUTH_DB, config.PostgresAuthDB)
	warnIfTransactionPooler(config_env.POSTGRES_USERS_DB, config.postgresUsersDB)

	return config
}

// SupabaseTransactionPoolerPort is PgBouncer in TRANSACTION pooling mode.
// Supabase serves session pooling on 5432 and transaction pooling on 6543.
const SupabaseTransactionPoolerPort = "6543"

// IsTransactionPoolerDSN reports whether a Postgres DSN points at a connection
// pooler running in transaction mode.
//
// This driver (lib/pq) speaks the extended query protocol and reuses the
// UNNAMED prepared statement for every query. Transaction pooling multiplexes
// client connections onto shared server connections between transactions, so a
// Parse issued for one query and the Bind that follows it can land on different
// server connections. The result is cross-talk between unrelated statements:
//
//	pq: bind message supplies 3 parameters, but prepared statement "" requires 2
//	pq: unnamed prepared statement does not exist
//
// WhatsMeow writes identity keys, signal sessions and app-state keys
// concurrently during the initial sync, which is exactly when this fires. The
// damage is silent and severe: app-state keys never persist (so the chat list
// never arrives), and peer messages cannot be encrypted (so history sync
// requests never leave the engine).
//
// Session pooling (port 5432) keeps one server connection per client connection
// for its whole life and is the supported endpoint for this driver.
func IsTransactionPoolerDSN(dsn string) bool {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return false
	}

	if strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return false
		}
		return u.Port() == SupabaseTransactionPoolerPort
	}

	// key=value DSN form: port=6543
	for _, field := range strings.Fields(trimmed) {
		if strings.EqualFold(field, "port="+SupabaseTransactionPoolerPort) {
			return true
		}
	}
	return false
}

func warnIfTransactionPooler(name, dsn string) {
	if !IsTransactionPoolerDSN(dsn) {
		return
	}
	logger.LogWarn(
		"[CONFIG] %s points at port %s, the TRANSACTION pooler. This driver reuses the unnamed prepared statement, "+
			"which transaction pooling corrupts: expect \"bind message supplies N parameters\" and \"unnamed prepared "+
			"statement does not exist\" errors, app-state keys that never persist (empty chat list) and history sync "+
			"requests that never reach WhatsApp. Use the session pooler on port 5432 instead.",
		name, SupabaseTransactionPoolerPort,
	)
}

func loadMinioConfig(config *Config) {
	minioEndpoint := os.Getenv(config_env.MINIO_ENDPOINT)
	panicIfEmpty(config_env.MINIO_ENDPOINT, minioEndpoint)

	minioAccessKey := os.Getenv(config_env.MINIO_ACCESS_KEY)
	panicIfEmpty(config_env.MINIO_ACCESS_KEY, minioAccessKey)

	minioSecretKey := os.Getenv(config_env.MINIO_SECRET_KEY)
	panicIfEmpty(config_env.MINIO_SECRET_KEY, minioSecretKey)

	minioBucket := os.Getenv(config_env.MINIO_BUCKET)
	panicIfEmpty(config_env.MINIO_BUCKET, minioBucket)

	minioUseSSL := os.Getenv(config_env.MINIO_USE_SSL) == "true"

	minioRegion := os.Getenv(config_env.MINIO_REGION)

	config.MinioEndpoint = minioEndpoint
	config.MinioAccessKey = minioAccessKey
	config.MinioSecretKey = minioSecretKey
	config.MinioBucket = minioBucket
	config.MinioUseSSL = minioUseSSL
	config.MinioRegion = minioRegion
}

func panicIfEmpty(key, value string) {
	if value == "" {
		if os.Getenv("DEBUG_ENABLED") != "1" {
			logger.LogInfo("You are NOT on development mode")
		}
		logger.LogFatal("[CONFIG] required configuration variable is missing. Please check your environment configuration.")
	}
}

// validateAMQPURL validates if the AMQP URL has the correct scheme and format
func validateAMQPURL(amqpURL string) error {
	if amqpURL == "" {
		return nil // Empty URL is allowed (RabbitMQ disabled)
	}

	// Parse the URL
	parsedURL, err := url.Parse(amqpURL)
	if err != nil {
		return fmt.Errorf("invalid AMQP URL format: %v", err)
	}

	// Check if scheme is valid
	if parsedURL.Scheme != "amqp" && parsedURL.Scheme != "amqps" {
		return fmt.Errorf("AMQP scheme must be either 'amqp://' or 'amqps://', got: '%s://'", parsedURL.Scheme)
	}

	// Check if host is present
	if parsedURL.Host == "" {
		return fmt.Errorf("AMQP URL must include a host")
	}

	logger.LogInfo("[CONFIG] AMQP URL validation successful: %s://%s", parsedURL.Scheme, parsedURL.Host)
	return nil
}
