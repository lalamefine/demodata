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
			{Type: "shuffler", Options: map[string]any{"column_names": []string{"name", "email"}}},
			{Type: "masker", Options: map[string]any{"column_name": "secret", "mask_char": "#"}},
			{Type: "generator", Options: map[string]any{"column_name": "id", "data_type": "integer"}},
		},
	}}}

	outDataset := ApplyRules(ingest.Dataset{"default": recs}, cfg, 42)
	out := outDataset["default"]

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

func TestApplyRulesShufflerLegacyCSV(t *testing.T) {
	recs := []ingest.Record{
		{"name": "Alice", "email": "a@x.com"},
		{"name": "Bob", "email": "b@x.com"},
	}

	cfg := &config.Config{Tables: []config.TableConfig{{
		Name: "default",
		Transformers: []config.TransformerConfig{
			{Type: "shuffler", Options: map[string]any{"column_names": "name,email"}},
		},
	}}}

	outDataset := ApplyRules(ingest.Dataset{"default": recs}, cfg, 7)
	out := outDataset["default"]

	if len(out) != 2 {
		t.Fatalf("expected 2 output records, got %d", len(out))
	}

	inputPairs := map[string]bool{"Alice:a@x.com": true, "Bob:b@x.com": true}
	for _, r := range out {
		pair := r["name"].(string) + ":" + r["email"].(string)
		if !inputPairs[pair] {
			t.Fatalf("expected shuffled pair to remain valid, got %s", pair)
		}
	}
}

func TestSampleDataset(t *testing.T) {
	records := make([]ingest.Record, 10)
	for i := range records {
		records[i] = ingest.Record{"id": i}
	}
	ds := ingest.Dataset{"users": records}

	// 50% → 5 enregistrements
	out := SampleDataset(ds, 0.5, 42)
	if len(out["users"]) != 5 {
		t.Fatalf("expected 5 records at 50%%, got %d", len(out["users"]))
	}

	// 100% → identique
	out100 := SampleDataset(ds, 1.0, 42)
	if len(out100["users"]) != 10 {
		t.Fatalf("expected 10 records at 100%%, got %d", len(out100["users"]))
	}

	// Reproductibilité
	outA := SampleDataset(ds, 0.5, 42)
	outB := SampleDataset(ds, 0.5, 42)
	for i := range outA["users"] {
		idA := outA["users"][i]["id"]
		idB := outB["users"][i]["id"]
		if idA != idB {
			t.Fatalf("non-reproductible: index %d → %v vs %v", i, idA, idB)
		}
	}
}

func TestFilterFKViolations(t *testing.T) {
	// 10 utilisateurs (IDs int64 1..10)
	users := make([]ingest.Record, 10)
	for i := range users {
		users[i] = ingest.Record{"id": int64(i + 1)}
	}
	// 20 commandes référençant user_ids 1..20 (10 orphelines)
	orders := make([]ingest.Record, 20)
	for i := range orders {
		orders[i] = ingest.Record{"id": int64(i + 1), "user_id": int64(i + 1)}
	}

	ds := ingest.Dataset{"users": users, "orders": orders}

	rels := []ingest.FKRelation{
		{ChildTable: "orders", ChildCol: "user_id", ParentTable: "users", ParentCol: "id"},
	}
	out := FilterFKViolations(ds, rels)

	// Seules les commandes avec user_id 1..10 doivent rester
	if len(out["orders"]) != 10 {
		t.Fatalf("expected 10 orders after FK filtering, got %d", len(out["orders"]))
	}
	for _, rec := range out["orders"] {
		uid := rec["user_id"].(int64)
		if uid < 1 || uid > 10 {
			t.Fatalf("orphaned order found with user_id=%d", uid)
		}
	}
	// La table parente ne doit pas être modifiée
	if len(out["users"]) != 10 {
		t.Fatalf("expected users to be untouched, got %d", len(out["users"]))
	}
}

func TestFilterFKViolationsNoOrphans(t *testing.T) {
	// Aucune orpheline : le filtre ne doit rien supprimer
	users := []ingest.Record{{"id": int64(1)}, {"id": int64(2)}}
	orders := []ingest.Record{
		{"id": int64(1), "user_id": int64(1)},
		{"id": int64(2), "user_id": int64(2)},
	}
	ds := ingest.Dataset{"users": users, "orders": orders}
	rels := []ingest.FKRelation{
		{ChildTable: "orders", ChildCol: "user_id", ParentTable: "users", ParentCol: "id"},
	}
	out := FilterFKViolations(ds, rels)
	if len(out["orders"]) != 2 {
		t.Fatalf("expected 2 orders unchanged, got %d", len(out["orders"]))
	}
}
