package export

import (
	"testing"

	"github.com/triboulin/demodata/pkg/ingest"
)

func TestCSVExporter(t *testing.T) {
	recs := []ingest.Record{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(25)},
	}
	b, err := (&CSVExporter{}).Export(recs)
	if err != nil {
		t.Fatalf("CSV export failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("expected non-empty CSV output")
	}
}

func TestJSONExporter(t *testing.T) {
	recs := []ingest.Record{
		{"id": int64(1), "name": "Alice"},
	}
	b, err := (&JSONExporter{}).Export(recs)
	if err != nil {
		t.Fatalf("JSON export failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("expected non-empty JSON output")
	}
}

func TestXLSXExporter(t *testing.T) {
	recs := []ingest.Record{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(25)},
	}
	b, err := (&XLSXExporter{}).Export(recs)
	if err != nil {
		t.Fatalf("XLSX export failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("expected non-empty XLSX output")
	}
}
