package config

import (
	"net/url"
	"strings"
	"testing"
)

func TestExtractDBNameAndAdminDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantDB  string
		wantErr bool
	}{
		{
			name:   "supabase IPv4 session pooler URL preserves SSL and tenant username",
			dsn:    "postgresql://postgres.projectref:password@aws-0-eu-west-3.pooler.supabase.com:5432/postgres?sslmode=require",
			wantDB: "postgres",
		},
		{
			name:   "supabase direct URL preserves SSL and host for migrations",
			dsn:    "postgresql://postgres:password@db.project-ref.supabase.co:5432/postgres?sslmode=require",
			wantDB: "postgres",
		},
		{
			name:   "non default database is redirected to maintenance database",
			dsn:    "postgres://user:password@db.example.test:5432/evolution?sslmode=require",
			wantDB: "evolution",
		},
		{
			name:    "empty DSN is rejected",
			dsn:     "",
			wantErr: true,
		},
		{
			name:    "malformed URL is rejected",
			dsn:     "postgresql://%zz",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDB, gotAdminDSN, err := extractDBNameAndAdminDSN(tt.dsn)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("extractDBNameAndAdminDSN() error = %v", err)
			}
			if gotDB != tt.wantDB {
				t.Fatalf("database name = %q, want %q", gotDB, tt.wantDB)
			}
			parsed, err := url.Parse(gotAdminDSN)
			if err != nil {
				t.Fatalf("admin DSN is not parseable: %v", err)
			}
			if parsed.Path != "/postgres" {
				t.Fatalf("admin path = %q, want /postgres", parsed.Path)
			}
			if parsed.Query().Get("sslmode") != "require" {
				t.Fatalf("sslmode = %q, want require", parsed.Query().Get("sslmode"))
			}
			if strings.Contains(gotAdminDSN, "evolution") {
				t.Fatalf("admin DSN still points at application database: %q", gotAdminDSN)
			}
		})
	}
}
