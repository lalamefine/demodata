package transform

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/triboulin/demodata/pkg/config"
	"github.com/triboulin/demodata/pkg/ingest"
)

func TestMasker(t *testing.T) {
	r := []ingest.Record{{"secret": "abcdef", "other": "x"}}
	m := &Masker{Column: "secret", MaskChar: "*"}
	out := m.Apply(r)

	if out[0]["secret"] != "******" {
		t.Fatalf("expected secret masked, got %v", out[0]["secret"])
	}
	if out[0]["other"] != "x" {
		t.Fatalf("expected other unchanged, got %v", out[0]["other"])
	}
}

func TestGenerator(t *testing.T) {
	r := []ingest.Record{{"id": 123, "name": "Alice"}}
	g := &Generator{Column: "id", DataType: "integer"}
	out := g.Apply(r)
	if out[0]["id"] != int64(0) {
		t.Fatalf("expected id replaced with 0, got %v", out[0]["id"])
	}
	if out[0]["name"] != "Alice" {
		t.Fatalf("expected name unchanged, got %v", out[0]["name"])
	}
}

func TestShuffleRecords(t *testing.T) {
	recs := []ingest.Record{
		{"name": "Alice", "email": "a@example.com", "keep": 1},
		{"name": "Bob", "email": "b@example.com", "keep": 2},
		{"name": "Carol", "email": "c@example.com", "keep": 3},
	}

	inputPairs := map[string]int{}
	for _, r := range recs {
		pair := r["name"].(string) + ":" + r["email"].(string)
		inputPairs[pair]++
	}

	out := ShuffleRecords(recs, []string{"name", "email"}, rand.New(rand.NewSource(42)))

	if len(out) != len(recs) {
		t.Fatalf("expected %d records, got %d", len(recs), len(out))
	}

	outPairs := map[string]int{}
	for _, r := range out {
		pair := r["name"].(string) + ":" + r["email"].(string)
		outPairs[pair]++
		if r["keep"] == 0 {
			t.Fatalf("expected keep column preserved and non-zero")
		}
	}

	if !reflect.DeepEqual(inputPairs, outPairs) {
		t.Fatalf("shuffled pairs should be permutation of input; got %v, want %v", outPairs, inputPairs)
	}
}

func TestApplyRules(t *testing.T) {
	recs := []ingest.Record{
		{"name": "Alice", "email": "a@x.com", "secret": "abc", "id": int64(1)},
		{"name": "Bob", "email": "b@x.com", "secret": "def", "id": int64(2)},
	}

	cfg := &config.Config{Tables: []config.TableConfig{{
		Name: "default",
		Transformers: []config.TransformerConfig{
			{Type: "shuffler", Options: map[string]any{"column_names": "name,email"}},
			{Type: "masker", Options: map[string]any{"column_name": "secret", "mask_char": "#"}},
			{Type: "generator", Options: map[string]any{"column_name": "id", "data_type": "integer"}},
		},
	}}}

	out := ApplyRules(recs, cfg, 42)

	if len(out) != 2 {
		t.Fatalf("expected 2 output records, got %d", len(out))
	}

	for _, r := range out {
		if r["secret"] != "###" {
			t.Fatalf("expected secret masked with ###, got %v", r["secret"])
		}
		if _, ok := r["id"].(int64); !ok {
			t.Fatalf("expected id generated as int64, got %T", r["id"])
		}
	}

	inputPairs := map[string]bool{"Alice:a@x.com": true, "Bob:b@x.com": true}
	found := 0
	for _, r := range out {
		pair := r["name"].(string) + ":" + r["email"].(string)
		if inputPairs[pair] {
			found++
		}
	}
	if found != 2 {
		t.Fatalf("expected shuffled bio pairs to be permutation of input, got output %v", out)
	}
}
