package export

import (
	"testing"

	"github.com/triboulin/demodata/pkg/ingest"
)

func TestExportToDBSQLite(t *testing.T) {
	db, err := ingest.OpenDB("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name) VALUES (10, 'to_delete')"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	dataset := ingest.Dataset{
		"users": {
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		},
	}
	if err := ExportToDB(db, dataset, "sqlite"); err != nil {
		t.Fatalf("export db: %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var gotIDs []int64
	var gotNames []string
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		gotIDs = append(gotIDs, id)
		gotNames = append(gotNames, name)
	}
	if len(gotIDs) != 2 || gotIDs[0] != 1 || gotIDs[1] != 2 {
		t.Fatalf("unexpected ids: %v", gotIDs)
	}
	if gotNames[0] != "Alice" || gotNames[1] != "Bob" {
		t.Fatalf("unexpected names: %v", gotNames)
	}
}
