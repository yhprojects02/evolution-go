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

func TestIsTransactionPoolerDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want bool
	}{
		{
			name: "supabase transaction pooler is rejected",
			dsn:  "postgresql://postgres.projectref:password@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres?sslmode=require",
			want: true,
		},
		{
			name: "supabase session pooler is the supported endpoint",
			dsn:  "postgresql://postgres.projectref:password@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres?sslmode=require",
			want: false,
		},
		{
			name: "postgres scheme is detected as well as postgresql",
			dsn:  "postgres://user:pass@host:6543/postgres",
			want: true,
		},
		{
			name: "key=value DSN carrying the transaction pooler port",
			dsn:  "host=aws-0-ap-southeast-1.pooler.supabase.com port=6543 dbname=postgres sslmode=require",
			want: true,
		},
		{
			name: "key=value DSN on the session pooler",
			dsn:  "host=aws-0-ap-southeast-1.pooler.supabase.com port=5432 dbname=postgres sslmode=require",
			want: false,
		},
		{
			name: "a database merely NAMED 6543 is not a pooler port",
			dsn:  "postgresql://user:pass@host:5432/6543",
			want: false,
		},
		{
			name: "default port is implicit and not the transaction pooler",
			dsn:  "postgresql://user:pass@host/postgres",
			want: false,
		},
		{name: "empty DSN", dsn: "", want: false},
		{name: "whitespace only", dsn: "   ", want: false},
		{name: "unparseable DSN does not panic", dsn: "postgres://user:p@ss w0rd@host:6543/db", want: false},
		{name: "sqlite DSN is not a pooler", dsn: "file:./dbdata/main.db?_pragma=foreign_keys(1)", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransactionPoolerDSN(tt.dsn); got != tt.want {
				t.Errorf("IsTransactionPoolerDSN(%q) = %v, want %v", tt.dsn, got, tt.want)
			}
		})
	}
}
