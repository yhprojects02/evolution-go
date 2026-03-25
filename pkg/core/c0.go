package core

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var _k1 = []byte{0x58, 0x18, 0x90, 0x2d, 0xba, 0x75, 0x41, 0xad, 0x68, 0xc3, 0xb6, 0x01, 0xed, 0x7d, 0x1d, 0x44, 0x07, 0xd3, 0x59, 0xc6, 0x61, 0x50, 0x62, 0x94, 0x8f, 0x7b, 0xa2, 0xb0, 0xa6, 0x97, 0xfe, 0x91, 0x0c, 0xdf, 0xf4, 0x99, 0x71, 0x54, 0xe9, 0x1b, 0x70, 0xf2}
var _k0 = []byte{0x30, 0x6c, 0xe4, 0x5d, 0xc9, 0x4f, 0x6e, 0x82, 0x04, 0xaa, 0xd5, 0x64, 0x83, 0x0e, 0x78, 0x6a, 0x62, 0xa5, 0x36, 0xaa, 0x14, 0x24, 0x0b, 0xfb, 0xe1, 0x1d, 0xcd, 0xc5, 0xc8, 0xf3, 0x9f, 0xe5, 0x65, 0xb0, 0x9a, 0xb7, 0x12, 0x3b, 0x84, 0x35, 0x12, 0x80}

var (
	_hb5 string
	_s3kc    string
)

func _53() string {
	if _hb5 != "" && _s3kc != "" {
		return _nc(_hb5, _s3kc)
	}
	parts := [...]string{"h", "tt", "ps", "://", "li", "ce", "nse", ".", "ev", "ol", "ut", "io", "nf", "ou", "nd", "at", "io", "n.", "co", "m.", "br"}
	var s string
	for _, p := range parts {
		s += p
	}
	return s
}

func _nc(enc, key string) string {
	encBytes := _jcv(enc)
	keyBytes := _jcv(key)
	if len(keyBytes) == 0 {
		return ""
	}
	out := make([]byte, len(encBytes))
	for i, b := range encBytes {
		out[i] = b ^ keyBytes[i%len(keyBytes)]
	}
	return string(out)
}

func _jcv(s string) []byte {
	if len(s)%2 != 0 {
		return nil
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		b[i/2] = _vcc(s[i])<<4 | _vcc(s[i+1])
	}
	return b
}

func _vcc(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

var _wof6 = &http.Client{Timeout: 10 * time.Second}

func _h33h(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func _cr(path string, payload interface{}, _34 string) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	url := _53() + path
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", _34)
	req.Header.Set("X-Signature", _h33h(body, _34))

	return _wof6.Do(req)
}

func _ml(path string) (*http.Response, error) {
	url := _53() + path
	return _wof6.Get(url)
}

func _i0(path string, payload interface{}) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	url := _53() + path
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return _wof6.Do(req)
}

func _lo(resp *http.Response) error {
	b, _ := io.ReadAll(resp.Body)
	var _286 struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(b, &_286); err == nil {
		msg := _286.Message
		if msg == "" {
			msg = _286.Error
		}
		if msg != "" {
			return fmt.Errorf("%s (HTTP %d)", strings.ToLower(msg), resp.StatusCode)
		}
	}
	return fmt.Errorf("HTTP %d", resp.StatusCode)
}

type RuntimeConfig struct {
	ID         uint      `gorm:"primaryKey;autoIncrement" json:"id"`
	Key        string    `gorm:"uniqueIndex;size:100;not null" json:"key"`
	Value      string    `gorm:"type:text;not null" json:"value"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (RuntimeConfig) TableName() string {
	return "runtime_configs"
}

const (
	ConfigKeyInstanceID = "instance_id"
	ConfigKeyAPIKey     = "api_key"
	ConfigKeyTier       = "tier"
	ConfigKeyCustomerID = "customer_id"
)

var _uj *gorm.DB

func SetDB(db *gorm.DB) {
	_uj = db
}

func MigrateDB() error {
	if _uj == nil {
		return fmt.Errorf("core: database not set, call SetDB first")
	}
	return _uj.AutoMigrate(&RuntimeConfig{})
}

func _vm5(key string) (string, error) {
	if _uj == nil {
		return "", fmt.Errorf("core: database not set")
	}
	var _vi5 RuntimeConfig
	_78g := _uj.Where("key = ?", key).First(&_vi5)
	if _78g.Error != nil {
		return "", _78g.Error
	}
	return _vi5.Value, nil
}

func _scrw(key, value string) error {
	if _uj == nil {
		return fmt.Errorf("core: database not set")
	}
	var _vi5 RuntimeConfig
	_78g := _uj.Where("key = ?", key).First(&_vi5)
	if _78g.Error != nil {
		return _uj.Create(&RuntimeConfig{Key: key, Value: value}).Error
	}
	return _uj.Model(&_vi5).Update("value", value).Error
}

func _979q(key string) {
	if _uj == nil {
		return
	}
	_uj.Where("key = ?", key).Delete(&RuntimeConfig{})
}

type RuntimeData struct {
	APIKey     string
	Tier       string
	CustomerID int
}

func _7x() (*RuntimeData, error) {
	_34, err := _vm5(ConfigKeyAPIKey)
	if err != nil || _34 == "" {
		return nil, fmt.Errorf("no license found")
	}

	_7j2, _ := _vm5(ConfigKeyTier)
	customerIDStr, _ := _vm5(ConfigKeyCustomerID)
	customerID, _ := strconv.Atoi(customerIDStr)

	return &RuntimeData{
		APIKey:     _34,
		Tier:       _7j2,
		CustomerID: customerID,
	}, nil
}

func _yi4m(rd *RuntimeData) error {
	if err := _scrw(ConfigKeyAPIKey, rd.APIKey); err != nil {
		return err
	}
	if err := _scrw(ConfigKeyTier, rd.Tier); err != nil {
		return err
	}
	if rd.CustomerID > 0 {
		if err := _scrw(ConfigKeyCustomerID, strconv.Itoa(rd.CustomerID)); err != nil {
			return err
		}
	}
	return nil
}

func _98g() {
	_979q(ConfigKeyAPIKey)
	_979q(ConfigKeyTier)
	_979q(ConfigKeyCustomerID)
}

func _h6vl() (string, error) {
	id, err := _vm5(ConfigKeyInstanceID)
	if err == nil && len(id) == 36 {
		return id, nil
	}

	id = _by()
	if id == "" {
		id, err = _xre()
		if err != nil {
			return "", err
		}
	}

	if err := _scrw(ConfigKeyInstanceID, id); err != nil {
		return "", err
	}
	return id, nil
}

func _by() string {
	hostname, _ := os.Hostname()
	macAddr := _qe5()
	if hostname == "" && macAddr == "" {
		return ""
	}

	seed := hostname + "|" + macAddr
	h := make([]byte, 16)
	copy(h, []byte(seed))
	for i := 16; i < len(seed); i++ {
		h[i%16] ^= seed[i]
	}
	h[6] = (h[6] & 0x0f) | 0x40 // _bwg 4
	h[8] = (h[8] & 0x3f) | 0x80 // variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

func _qe5() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}
		if len(iface.HardwareAddr) > 0 {
			return iface.HardwareAddr.String()
		}
	}
	return ""
}

func _xre() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

var _qr5 atomic.Value // set during activation

func init() {
	_qr5.Store([]byte{0})
}

func ComputeSessionSeed(instanceName string, rc *RuntimeContext) []byte {
	if rc == nil || !rc._dcf.Load() {
		return nil // Will cause panic in caller — intentional
	}
	h := sha256.New()
	h.Write([]byte(instanceName))
	h.Write([]byte(rc._34))
	salt, _ := _qr5.Load().([]byte)
	h.Write(salt)
	return h.Sum(nil)[:16]
}

func ValidateRouteAccess(rc *RuntimeContext) uint64 {
	if rc == nil {
		return 0
	}
	h := rc.ContextHash()
	return binary.LittleEndian.Uint64(h[:8])
}

func DeriveInstanceToken(_tpx0 string, rc *RuntimeContext) string {
	if rc == nil || !rc._dcf.Load() {
		return ""
	}
	h := sha256.Sum256([]byte(_tpx0 + rc._34))
	return _aj(h[:8])
}

func _aj(b []byte) string {
	const _oww0 = "0123456789abcdef"
	dst := make([]byte, len(b)*2)
	for i, v := range b {
		dst[i*2] = _oww0[v>>4]
		dst[i*2+1] = _oww0[v&0x0f]
	}
	return string(dst)
}

func ActivateIntegrity(rc *RuntimeContext) {
	if rc == nil {
		return
	}
	h := sha256.Sum256([]byte(rc._34 + rc._tpx0 + "ev0"))
	_qr5.Store(h[:])
}

const (
	hbInterval = 30 * time.Minute
)

type RuntimeContext struct {
	_34       string
	_t0 string // GLOBAL_API_KEY from .env — used as token for licensing check
	_tpx0   string
	_dcf       atomic.Bool
	_tq      [32]byte // Derived from activation — required by ValidateContext
	mu           sync.RWMutex
	_xbaq       string // Registration URL shown to users before activation
	_8w9u     string // Registration token for polling
	_7j2         string
	_bwg      string
}

func (rc *RuntimeContext) ContextHash() [32]byte {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc._tq
}

func (rc *RuntimeContext) IsActive() bool {
	return rc._dcf.Load()
}

func (rc *RuntimeContext) RegistrationURL() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc._xbaq
}

func (rc *RuntimeContext) APIKey() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc._34
}

func (rc *RuntimeContext) InstanceID() string {
	return rc._tpx0
}

func InitializeRuntime(_7j2, _bwg, _t0 string) *RuntimeContext {
	if _7j2 == "" {
		_7j2 = "evolution-go"
	}
	if _bwg == "" {
		_bwg = "unknown"
	}

	rc := &RuntimeContext{
		_7j2:         _7j2,
		_bwg:      _bwg,
		_t0: _t0,
	}

	id, err := _h6vl()
	if err != nil {
		log.Fatalf("[runtime] failed to initialize instance: %v", err)
	}
	rc._tpx0 = id

	rd, err := _7x()
	if err == nil && rd.APIKey != "" {
		rc._34 = rd.APIKey
		fmt.Printf("  ✓ License found: %s...%s\n", rd.APIKey[:8], rd.APIKey[len(rd.APIKey)-4:])

		rc._tq = sha256.Sum256([]byte(rc._34 + rc._tpx0))
		rc._dcf.Store(true)
		ActivateIntegrity(rc)
		fmt.Println("  ✓ License activated successfully")

		go func() {
			if err := _iljt(rc, _bwg); err != nil {
				fmt.Printf("  ⚠ Remote activation notice failed (non-blocking): %v\n", err)
			}
		}()
	} else if rc._t0 != "" {
		rc._34 = rc._t0
		if err := _iljt(rc, _bwg); err == nil {
			_yi4m(&RuntimeData{APIKey: rc._t0, Tier: _7j2})
			rc._tq = sha256.Sum256([]byte(rc._34 + rc._tpx0))
			rc._dcf.Store(true)
			ActivateIntegrity(rc)
			fmt.Printf("  ✓ GLOBAL_API_KEY accepted — license saved and activated\n")
		} else {
			rc._34 = ""
			_v9()
			rc._dcf.Store(false)
		}
	} else {
		_v9()
		rc._dcf.Store(false)
	}

	return rc
}

func _v9() {
	fmt.Println()
	fmt.Println("  ╔══════════════════════════════════════════════════════════╗")
	fmt.Println("  ║              License Registration Required               ║")
	fmt.Println("  ╚══════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("  Server starting without license.")
	fmt.Println("  API endpoints will return 503 until license is activated.")
	fmt.Println("  Use GET /license/register to get the registration URL.")
	fmt.Println()
}

func (rc *RuntimeContext) _cqd(authCodeOrKey, _7j2 string, customerID int) error {
	_34, err := _on(authCodeOrKey)
	if err != nil {
		return fmt.Errorf("key exchange failed: %w", err)
	}

	rc.mu.Lock()
	rc._34 = _34
	rc._xbaq = ""
	rc._8w9u = ""
	rc.mu.Unlock()

	if err := _yi4m(&RuntimeData{
		APIKey:     _34,
		Tier:       _7j2,
		CustomerID: customerID,
	}); err != nil {
		fmt.Printf("  ⚠ Warning: could not save license: %v\n", err)
	}

	if err := _iljt(rc, rc._bwg); err != nil {
		return err
	}

	rc.mu.Lock()
	rc._tq = sha256.Sum256([]byte(rc._34 + rc._tpx0))
	rc.mu.Unlock()
	rc._dcf.Store(true)
	ActivateIntegrity(rc)

	fmt.Printf("  ✓ License activated! Key: %s...%s (_7j2: %s)\n",
		_34[:8], _34[len(_34)-4:], _7j2)

	go func() {
		if err := _727r(rc, 0); err != nil {
			fmt.Printf("  ⚠ First heartbeat failed: %v\n", err)
		}
	}()

	return nil
}

func ValidateContext(rc *RuntimeContext) (bool, string) {
	if rc == nil {
		return false, ""
	}
	if !rc._dcf.Load() {
		return false, rc.RegistrationURL()
	}
	expected := sha256.Sum256([]byte(rc._34 + rc._tpx0))
	actual := rc.ContextHash()
	if expected != actual {
		return false, ""
	}
	return true, ""
}

func GateMiddleware(rc *RuntimeContext) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path

		if path == "/health" || path == "/server/ok" || path == "/favicon.ico" ||
			path == "/license/status" || path == "/license/register" || path == "/license/activate" ||
			strings.HasPrefix(path, "/manager") || strings.HasPrefix(path, "/assets") ||
			strings.HasPrefix(path, "/swagger") || path == "/ws" ||
			strings.HasSuffix(path, ".svg") || strings.HasSuffix(path, ".css") ||
			strings.HasSuffix(path, ".js") || strings.HasSuffix(path, ".png") ||
			strings.HasSuffix(path, ".ico") || strings.HasSuffix(path, ".woff2") ||
			strings.HasSuffix(path, ".woff") || strings.HasSuffix(path, ".ttf") {
			c.Next()
			return
		}

		valid, _ := ValidateContext(rc)
		if !valid {
			scheme := "http"
			if c.Request.TLS != nil {
				scheme = "https"
			}
			managerURL := fmt.Sprintf("%s://%s/manager/login", scheme, c.Request.Host)

			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error":        "service not activated",
				"code":         "LICENSE_REQUIRED",
				"register_url": managerURL,
				"message":      "License required. Open the manager to activate your license.",
			})
			return
		}

		c.Set("_rch", rc.ContextHash())
		c.Next()
	}
}

func LicenseRoutes(eng *gin.Engine, rc *RuntimeContext) {
	lic := eng.Group("/license")
	{
		lic.GET("/status", func(c *gin.Context) {
			status := "inactive"
			if rc.IsActive() {
				status = "active"
			}

			resp := gin.H{
				"status":      status,
				"instance_id": rc._tpx0,
			}

			rc.mu.RLock()
			if rc._34 != "" {
				resp["api_key"] = rc._34[:8] + "..." + rc._34[len(rc._34)-4:]
			}
			rc.mu.RUnlock()

			c.JSON(http.StatusOK, resp)
		})

		lic.GET("/register", func(c *gin.Context) {
			if rc.IsActive() {
				c.JSON(http.StatusOK, gin.H{
					"status":  "active",
					"message": "License is already active",
				})
				return
			}

			rc.mu.RLock()
			existingURL := rc._xbaq
			rc.mu.RUnlock()

			if existingURL != "" {
				c.JSON(http.StatusOK, gin.H{
					"status":       "pending",
					"register_url": existingURL,
				})
				return
			}

			payload := map[string]string{
				"tier":        rc._7j2,
				"version":     rc._bwg,
				"instance_id": rc._tpx0,
			}
			if redirectURI := c.Query("redirect_uri"); redirectURI != "" {
				payload["redirect_uri"] = redirectURI
			}

			resp, err := _i0("/v1/register/init", payload)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{
					"error":   "Failed to contact licensing server",
					"details": err.Error(),
				})
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				_286 := _lo(resp)
				c.JSON(resp.StatusCode, gin.H{
					"error":   "Licensing server error",
					"details": _286.Error(),
				})
				return
			}

			var _0ijb struct {
				RegisterURL string `json:"register_url"`
				Token       string `json:"token"`
			}
			json.NewDecoder(resp.Body).Decode(&_0ijb)

			rc.mu.Lock()
			rc._xbaq = _0ijb.RegisterURL
			rc._8w9u = _0ijb.Token
			rc.mu.Unlock()

			fmt.Printf("  → Registration URL: %s\n", _0ijb.RegisterURL)

			c.JSON(http.StatusOK, gin.H{
				"status":       "pending",
				"register_url": _0ijb.RegisterURL,
			})
		})

		lic.GET("/activate", func(c *gin.Context) {
			if rc.IsActive() {
				c.JSON(http.StatusOK, gin.H{
					"status":  "active",
					"message": "License is already active",
				})
				return
			}

			code := c.Query("code")
			if code == "" {
				c.JSON(http.StatusBadRequest, gin.H{
					"error":   "Missing code parameter",
					"message": "Provide ?code=AUTHORIZATION_CODE from the registration callback.",
				})
				return
			}

			exchangeResp, err := _i0("/v1/register/exchange", map[string]string{
				"authorization_code": code,
				"instance_id":       rc._tpx0,
			})
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{
					"error":   "Failed to contact licensing server",
					"details": err.Error(),
				})
				return
			}
			defer exchangeResp.Body.Close()

			if exchangeResp.StatusCode != http.StatusOK {
				_286 := _lo(exchangeResp)
				c.JSON(exchangeResp.StatusCode, gin.H{
					"error":   "Exchange failed",
					"details": _286.Error(),
				})
				return
			}

			var _78g struct {
				APIKey     string `json:"api_key"`
				Tier       string `json:"tier"`
				CustomerID int    `json:"customer_id"`
			}
			json.NewDecoder(exchangeResp.Body).Decode(&_78g)

			if _78g.APIKey == "" {
				c.JSON(http.StatusBadRequest, gin.H{
					"error":   "Invalid or expired code",
					"message": "The authorization code is invalid or has expired.",
				})
				return
			}

			if err := rc._cqd(_78g.APIKey, _78g.Tier, _78g.CustomerID); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error":   "Activation failed",
					"details": err.Error(),
				})
				return
			}

			c.JSON(http.StatusOK, gin.H{
				"status":  "active",
				"message": "License activated successfully!",
			})
		})
	}
}

func StartHeartbeat(ctx context.Context, rc *RuntimeContext, startTime time.Time) {
	go func() {
		ticker := time.NewTicker(hbInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !rc.IsActive() {
					continue
				}
				uptime := int64(time.Since(startTime).Seconds())
				if err := _727r(rc, uptime); err != nil {
					fmt.Printf("  ⚠ Heartbeat failed (non-blocking): %v\n", err)
				}
			}
		}
	}()
}

func Shutdown(rc *RuntimeContext) {
	if rc == nil || rc._34 == "" {
		return
	}
	_4dua(rc)
}

func _gxkr(code string) (_34 string, err error) {
	resp, err := _i0("/v1/register/exchange", map[string]string{
		"authorization_code": code,
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", _lo(resp)
	}

	var _78g struct {
		APIKey string `json:"api_key"`
	}
	json.NewDecoder(resp.Body).Decode(&_78g)
	if _78g.APIKey == "" {
		return "", fmt.Errorf("exchange returned empty api_key")
	}
	return _78g.APIKey, nil
}

func _on(authCodeOrKey string) (string, error) {
	_34, err := _gxkr(authCodeOrKey)
	if err == nil && _34 != "" {
		return _34, nil
	}
	return authCodeOrKey, nil
}

func _iljt(rc *RuntimeContext, _bwg string) error {
	resp, err := _cr("/v1/activate", map[string]string{
		"instance_id": rc._tpx0,
		"version":     _bwg,
	}, rc._34)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return _lo(resp)
	}

	var _78g struct {
		Status string `json:"status"`
	}
	json.NewDecoder(resp.Body).Decode(&_78g)

	if _78g.Status != "active" {
		return fmt.Errorf("activation returned status: %s", _78g.Status)
	}
	return nil
}

func _727r(rc *RuntimeContext, uptimeSeconds int64) error {
	resp, err := _cr("/v1/heartbeat", map[string]any{
		"instance_id":    rc._tpx0,
		"uptime_seconds": uptimeSeconds,
		"version":        rc._bwg,
	}, rc._34)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return _lo(resp)
	}
	return nil
}

func _4dua(rc *RuntimeContext) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, _ := json.Marshal(map[string]string{
		"instance_id": rc._tpx0,
	})

	url := _53() + "/v1/deactivate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", rc._34)
	req.Header.Set("X-Signature", _h33h(body, rc._34))
	_wof6.Do(req)
}
