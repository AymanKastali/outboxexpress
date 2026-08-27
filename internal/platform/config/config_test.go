package config

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadAPI_Defaults(t *testing.T) {
	got, err := LoadAPI(env(map[string]string{
		"ACCOUNTS_DATABASE_URL": "postgres://oe:oe@localhost:5432/oe_accounts",
	}))
	if err != nil {
		t.Fatalf("LoadAPI: %v", err)
	}
	if got.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", got.HTTPAddr, ":8080")
	}
	if got.AdminAddr != "127.0.0.1:8081" {
		t.Errorf("AdminAddr = %q, want %q", got.AdminAddr, "127.0.0.1:8081")
	}
	if got.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want %v", got.LogLevel, slog.LevelInfo)
	}
}

func TestLoadAPI_Overrides(t *testing.T) {
	got, err := LoadAPI(env(map[string]string{
		"ACCOUNTS_DATABASE_URL": "postgres://x/y",
		"HTTP_ADDR":             ":9090",
		"ADMIN_ADDR":            "127.0.0.1:9091",
		"LOG_LEVEL":             "debug",
	}))
	if err != nil {
		t.Fatalf("LoadAPI: %v", err)
	}
	if got.HTTPAddr != ":9090" || got.AdminAddr != "127.0.0.1:9091" || got.LogLevel != slog.LevelDebug {
		t.Errorf("overrides not applied: %+v", got)
	}
}

func TestLoadAPI_Errors(t *testing.T) {
	tests := []struct {
		name string
		envs map[string]string
		want error
	}{
		{
			name: "missing database url",
			envs: map[string]string{},
			want: ErrMissing,
		},
		{
			name: "blank database url",
			envs: map[string]string{"ACCOUNTS_DATABASE_URL": "   "},
			want: ErrMissing,
		},
		{
			name: "unknown log level",
			envs: map[string]string{"ACCOUNTS_DATABASE_URL": "postgres://x/y", "LOG_LEVEL": "chatty"},
			want: ErrInvalid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadAPI(env(tc.envs))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestLoadAPI_ErrorNamesTheVariable(t *testing.T) {
	_, err := LoadAPI(env(map[string]string{}))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "ACCOUNTS_DATABASE_URL") {
		t.Fatalf("error %q does not name the offending variable", err)
	}
}

func TestLoadMigrate_RequiresBothURLs(t *testing.T) {
	_, err := LoadMigrate(env(map[string]string{"ACCOUNTS_DATABASE_URL": "postgres://x/y"}))
	if !errors.Is(err, ErrMissing) {
		t.Fatalf("err = %v, want ErrMissing", err)
	}
	got, err := LoadMigrate(env(map[string]string{
		"ACCOUNTS_DATABASE_URL":      "postgres://x/a",
		"NOTIFICATIONS_DATABASE_URL": "postgres://x/n",
	}))
	if err != nil {
		t.Fatalf("LoadMigrate: %v", err)
	}
	if got.NotificationsDatabaseURL != "postgres://x/n" {
		t.Errorf("NotificationsDatabaseURL = %q", got.NotificationsDatabaseURL)
	}
}
