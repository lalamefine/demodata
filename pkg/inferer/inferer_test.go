package inferer

import (
	"testing"

	"github.com/triboulin/demodata/pkg/ingest"
)

func TestInferRuleSet(t *testing.T) {
	recs := []ingest.Record{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com", "secret": "s1"},
		{"id": int64(2), "name": "Bob", "email": "bob@example.com", "secret": "s2"},
	}

	cfg := InferRuleSet(ingest.Dataset{"default": recs}, nil)
	if cfg == nil || len(cfg.Tables) != 1 {
		t.Fatalf("expected 1 table rule, got %+v", cfg)
	}

	trans := cfg.Tables[0].Transformers
	if len(trans) < 2 {
		t.Fatalf("expected au moins 2 transformers, got %d", len(trans))
	}

	foundSecret := false
	foundEmail := false
	foundID := false
	for _, tcfg := range trans {
		if tcfg.Type == "none" && tcfg.Options["column_name"] == "secret" {
			foundSecret = true
		}
		if tcfg.Type == "none" && tcfg.Options["column_name"] == "email" {
			foundEmail = true
		}
		if tcfg.Type == "none" && tcfg.Options["column_name"] == "id" {
			foundID = true
		}
	}

	if !foundSecret {
		t.Fatal("expected non-modified rule for secret")
	}
	if !foundEmail {
		t.Fatal("expected non-modified rule for email")
	}
	if !foundID {
		t.Fatal("expected non-modified rule for id")
	}
}

func TestInferRuleSetEmpty(t *testing.T) {
	cfg := InferRuleSet(ingest.Dataset{}, nil)
	if cfg == nil || len(cfg.Tables) != 0 {
		t.Fatalf("expected empty config for no records, got %+v", cfg)
	}
}
