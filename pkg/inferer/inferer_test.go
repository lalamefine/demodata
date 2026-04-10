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

	cfg := InferRuleSet(recs)
	if cfg == nil || len(cfg.Tables) != 1 {
		t.Fatalf("expected 1 table rule, got %+v", cfg)
	}

	trans := cfg.Tables[0].Transformers
	if len(trans) < 2 {
		t.Fatalf("expected au moins 2 transformers, got %d", len(trans))
	}

	foundMasker := false
	foundEmail := false
	foundIDGen := false
	for _, tcfg := range trans {
		if tcfg.Type == "sampler" && tcfg.Options["column_name"] == "secret" {
			foundMasker = true
		}
		if tcfg.Type == "sampler" && tcfg.Options["column_name"] == "email" {
			foundEmail = true
		}
		if tcfg.Type == "sampler" && tcfg.Options["column_name"] == "id" {
			foundIDGen = true
		}
	}

	if !foundMasker {
		t.Fatal("expected masker rule for secret")
	}
	if !foundEmail {
		t.Fatal("expected generator for email format")
	}
	if !foundIDGen {
		t.Fatal("expected generator for id")
	}
}

func TestInferRuleSetEmpty(t *testing.T) {
	cfg := InferRuleSet([]ingest.Record{})
	if cfg == nil || len(cfg.Tables) != 0 {
		t.Fatalf("expected empty config for no records, got %+v", cfg)
	}
}
