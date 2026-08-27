package config

import (
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
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

func TestLoadRelay_Defaults(t *testing.T) {
	env := map[string]string{
		"ACCOUNTS_DATABASE_URL": "postgres://oe:oe@localhost:5432/oe_accounts",
	}
	cfg, err := LoadRelay(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadRelay: %v", err)
	}

	// Spec §14's table, verbatim.
	if got, want := cfg.KafkaBrokers, []string{"localhost:9092"}; !slices.Equal(got, want) {
		t.Errorf("KafkaBrokers = %v, want %v", got, want)
	}
	if cfg.KafkaTopic != "accounts.user.v1" {
		t.Errorf("KafkaTopic = %q, want accounts.user.v1", cfg.KafkaTopic)
	}
	// §13.5: "The admin listener binds loopback, not a wildcard."
	if cfg.AdminAddr != "127.0.0.1:8082" {
		t.Errorf("AdminAddr = %q, want 127.0.0.1:8082", cfg.AdminAddr)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", cfg.BatchSize)
	}
	if cfg.IdleMin != 50*time.Millisecond || cfg.IdleMax != 2*time.Second {
		t.Errorf("idle = %v/%v, want 50ms/2s", cfg.IdleMin, cfg.IdleMax)
	}
	if cfg.BackoffBase != time.Second || cfg.BackoffCap != 5*time.Minute {
		t.Errorf("backoff = %v/%v, want 1s/5m", cfg.BackoffBase, cfg.BackoffCap)
	}
	if cfg.MaxAttempts != 10 {
		t.Errorf("MaxAttempts = %d, want 10", cfg.MaxAttempts)
	}
	if !cfg.UseNotify {
		t.Error("UseNotify = false, want true by default")
	}
	// Comfortably inside Kubernetes' 30s default terminationGracePeriodSeconds,
	// and enough for one produce at §10.1's 10s request timeout plus its marks.
	// A zero here would silently restore the behaviour where every restart
	// republishes whatever the pass in flight had already acked.
	if cfg.DrainGrace != 15*time.Second {
		t.Errorf("DrainGrace = %v, want 15s", cfg.DrainGrace)
	}
	if cfg.PurgeInterval != time.Minute || cfg.OutboxRetention != 24*time.Hour || cfg.PurgeBatch != 1000 {
		t.Errorf("purge = %v/%v/%d, want 1m/24h/1000",
			cfg.PurgeInterval, cfg.OutboxRetention, cfg.PurgeBatch)
	}
}

func TestLoadRelay_ReadsEveryVariable(t *testing.T) {
	env := map[string]string{
		"ACCOUNTS_DATABASE_URL": "postgres://oe:oe@db:5432/oe_accounts",
		"KAFKA_BROKERS":         "a:9092, b:9092 ,c:9092",
		"KAFKA_TOPIC":           "accounts.user.v2",
		"ADMIN_ADDR":            "127.0.0.1:9999",
		"LOG_LEVEL":             "debug",
		"RELAY_BATCH_SIZE":      "50",
		"RELAY_IDLE_MIN":        "10ms",
		"RELAY_IDLE_MAX":        "5s",
		"RELAY_BACKOFF_BASE":    "2s",
		"RELAY_BACKOFF_CAP":     "10m",
		"RELAY_MAX_ATTEMPTS":    "3",
		"RELAY_USE_NOTIFY":      "false",
		"RELAY_DRAIN_GRACE":     "8s",
		"PURGE_INTERVAL":        "30s",
		"OUTBOX_RETENTION":      "72h",
		"PURGE_BATCH":           "250",
	}
	cfg, err := LoadRelay(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadRelay: %v", err)
	}

	// A broker list is comma-separated with whitespace nobody should have to
	// think about.
	if got, want := cfg.KafkaBrokers, []string{"a:9092", "b:9092", "c:9092"}; !slices.Equal(got, want) {
		t.Errorf("KafkaBrokers = %v, want %v", got, want)
	}
	if cfg.UseNotify {
		t.Error("UseNotify = true, want RELAY_USE_NOTIFY=false to be honoured")
	}
	if cfg.MaxAttempts != 3 || cfg.PurgeBatch != 250 || cfg.BatchSize != 50 {
		t.Errorf("counts = %d/%d/%d, want 3/250/50", cfg.MaxAttempts, cfg.PurgeBatch, cfg.BatchSize)
	}
	if cfg.OutboxRetention != 72*time.Hour {
		t.Errorf("OutboxRetention = %v, want 72h", cfg.OutboxRetention)
	}
	if cfg.DrainGrace != 8*time.Second {
		t.Errorf("DrainGrace = %v, want 8s", cfg.DrainGrace)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want debug", cfg.LogLevel)
	}
}

// A process that starts with invalid configuration fails later and further from
// the cause. Fifteen variables is enough that reporting them one boot at a time
// is a bad trade, so LoadRelay joins them.
func TestLoadRelay_ReportsEveryProblemAtOnce(t *testing.T) {
	env := map[string]string{
		"KAFKA_BROKERS":      "",
		"RELAY_BATCH_SIZE":   "0",
		"RELAY_IDLE_MIN":     "5s",
		"RELAY_IDLE_MAX":     "1s", // below the minimum
		"RELAY_BACKOFF_BASE": "10m",
		"RELAY_BACKOFF_CAP":  "1s", // below the base
		"RELAY_MAX_ATTEMPTS": "0",
		"RELAY_DRAIN_GRACE":  "0s",
		"PURGE_BATCH":        "-1",
	}
	_, err := LoadRelay(func(key string) string { return env[key] })
	if err == nil {
		t.Fatal("LoadRelay accepted a configuration with nine problems in it")
	}
	for _, want := range []string{
		"ACCOUNTS_DATABASE_URL",
		"RELAY_BATCH_SIZE",
		"RELAY_DRAIN_GRACE",
		"RELAY_IDLE_MAX",
		"RELAY_BACKOFF_CAP",
		"RELAY_MAX_ATTEMPTS",
		"PURGE_BATCH",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err does not mention %s:\n%v", want, err)
		}
	}
}

func TestLoadRelay_RejectsUnparseableValues(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"RELAY_BATCH_SIZE", "lots"},
		{"RELAY_IDLE_MIN", "quickly"},
		{"RELAY_USE_NOTIFY", "perhaps"},
		{"OUTBOX_RETENTION", "a while"},
	} {
		env := map[string]string{
			"ACCOUNTS_DATABASE_URL": "postgres://oe:oe@localhost:5432/oe_accounts",
			tc.key:                  tc.value,
		}
		_, err := LoadRelay(func(key string) string { return env[key] })
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s=%q: err = %v, want ErrInvalid", tc.key, tc.value, err)
		}
		if err != nil && !strings.Contains(err.Error(), tc.key) {
			t.Errorf("%s=%q: err = %q, want it to name the variable", tc.key, tc.value, err)
		}
	}
}
