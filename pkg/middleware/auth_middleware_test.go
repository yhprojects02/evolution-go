package auth_middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/EvolutionAPI/evolution-go/pkg/config"
	"github.com/gin-gonic/gin"
)

func TestAuthAdminRejectsInstanceKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/call/deploy-drain", NewMiddleware(&config.Config{GlobalApiKey: "global-release-key"}, nil).AuthAdmin, func(c *gin.Context) { c.Status(http.StatusNoContent) })
	for _, tc := range []struct {
		key  string
		want int
	}{{"instance-key", http.StatusUnauthorized}, {"global-release-key", http.StatusNoContent}} {
		req := httptest.NewRequest(http.MethodPost, "/call/deploy-drain", nil)
		req.Header.Set("apikey", tc.key)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if response.Code != tc.want {
			t.Fatalf("apikey result = %d, want %d", response.Code, tc.want)
		}
	}
}
