package offsetkeeper

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func validConfig() Config {
	return Config{
		Brokers: []string{"b:9092"},
		Group:   "g",
		Topics:  []string{"t"},
		Handler: func(context.Context, *kgo.Record) error { return nil },
	}
}

func TestConfigDefaults(t *testing.T) {
	c, err := validConfig().withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxAttempts != 3 || c.Backoff != 100*time.Millisecond || c.Logger != slog.Default() {
		t.Fatalf("bad defaults: %+v", c)
	}
	in := validConfig()
	in.MaxAttempts, in.Backoff = 1, time.Second
	c, _ = in.withDefaults()
	if c.MaxAttempts != 1 || c.Backoff != time.Second {
		t.Fatalf("explicit values overwritten: %+v", c)
	}
}

func TestConfigValidation(t *testing.T) {
	for name, mut := range map[string]func(*Config){
		"brokers":     func(c *Config) { c.Brokers = nil },
		"group":       func(c *Config) { c.Group = "" },
		"topics":      func(c *Config) { c.Topics = nil },
		"handler":     func(c *Config) { c.Handler = nil },
		"maxattempts": func(c *Config) { c.MaxAttempts = -1 },
		"backoff":     func(c *Config) { c.Backoff = -1 },
	} {
		c := validConfig()
		mut(&c)
		if _, err := c.withDefaults(); err == nil {
			t.Errorf("%s: want error", name)
		}
		if err := Run(context.Background(), c); err == nil {
			t.Errorf("%s: Run must reject invalid config, got %v", name, err)
		}
	}
}

type customErr struct{}

func (*customErr) Error() string { return "custom" }

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	base := fs.ErrNotExist
	err := Permanent(base)
	var p *permanentError
	if !errors.Is(err, base) || !errors.As(err, &p) || err.Error() != base.Error() {
		t.Fatalf("Is/As/Error broken: %v", err)
	}
	var ce *customErr
	if !errors.As(Permanent(&customErr{}), &ce) {
		t.Fatal("errors.As cannot see the wrapped type")
	}
}
