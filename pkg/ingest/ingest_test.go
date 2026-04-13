package ingest

import (
	"bytes"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestLoadCSV(t *testing.T) {
	csv := `id,name,age,active
1,Alice,30,true
2,Bob,25,false
`
	ds, _, err := LoadCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}
	recs := ds[DefaultTableName]
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[0]["name"] != "Alice" {
		t.Fatalf("expected name Alice, got %v", recs[0]["name"])
	}
	if recs[0]["age"] != int64(30) {
		t.Fatalf("expected age 30, got %v", recs[0]["age"])
	}
	if recs[0]["active"] != true {
		t.Fatalf("expected active true, got %v", recs[0]["active"])
	}
}

func TestLoadJSON(t *testing.T) {
	jsonData := `[
  {"id": 1, "name": "Alice", "age": 30, "active": true},
  {"id": 2, "name": "Bob", "age": 25, "active": false}
]`
	ds, _, err := LoadJSON(strings.NewReader(jsonData))
	if err != nil {
		t.Fatalf("LoadJSON failed: %v", err)
	}
	recs := ds[DefaultTableName]
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[1]["name"] != "Bob" {
		t.Fatalf("expected name Bob, got %v", recs[1]["name"])
	}
	if recs[1]["age"] != int64(25) {
		t.Fatalf("expected age 25, got %v", recs[1]["age"])
	}
	if recs[1]["active"] != false {
		t.Fatalf("expected active false, got %v", recs[1]["active"])
	}
}

func TestLoadXLSX(t *testing.T) {
	f := excelize.NewFile()
	idx, err := f.NewSheet("Sheet1")
	if err != nil {
		t.Fatalf("failed to create sheet: %v", err)
	}
	// En-tête
	f.SetCellValue("Sheet1", "A1", "id")
	f.SetCellValue("Sheet1", "B1", "name")
	f.SetCellValue("Sheet1", "C1", "age")
	// Données
	f.SetCellValue("Sheet1", "A2", 1)
	f.SetCellValue("Sheet1", "B2", "Alice")
	f.SetCellValue("Sheet1", "C2", 30)
	f.SetCellValue("Sheet1", "A3", 2)
	f.SetCellValue("Sheet1", "B3", "Bob")
	f.SetCellValue("Sheet1", "C3", 25)
	f.SetActiveSheet(idx)

	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		t.Fatalf("failed to write xlsx buffer: %v", err)
	}

	ds, _, err := LoadXLSX(&buf)
	if err != nil {
		t.Fatalf("LoadXLSX failed: %v", err)
	}
	recs := ds["Sheet1"]
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[1]["name"] != "Bob" {
		t.Fatalf("expected name Bob, got %v", recs[1]["name"])
	}
	if recs[1]["age"] != int64(25) {
		t.Fatalf("expected age 25, got %v", recs[1]["age"])
	}
}

func TestLoadWrapper(t *testing.T) {
	// CSV
	csv := "id,name,age\n1,Alice,30\n"
	ds, _, err := Load(strings.NewReader(csv), "csv")
	if err != nil {
		t.Fatalf("Load csv failed: %v", err)
	}
	recs := ds[DefaultTableName]
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}

	// JSON
	jsonData := `[{"id":1,"name":"Bob","age":25}]`
	ds, _, err = Load(strings.NewReader(jsonData), "json")
	if err != nil {
		t.Fatalf("Load json failed: %v", err)
	}
	recs = ds[DefaultTableName]
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}

	// Unsupported format
	_, _, err = Load(strings.NewReader(""), "xml")
	if err == nil {
		t.Fatalf("expected error for unsupported format")
	}
}
