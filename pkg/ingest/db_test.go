package ingest

import "testing"

func TestLoadDBSQLite(t *testing.T) {
	db, err := OpenDB("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	tables, err := ListTables(db, "sqlite")
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(tables) != 1 || tables[0] != "users" {
		t.Fatalf("unexpected tables: %v", tables)
	}

	dataset, err := LoadDB(db, "sqlite", nil)
	if err != nil {
		t.Fatalf("load db: %v", err)
	}
	recs := dataset["users"]
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[0]["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", recs[0]["name"])
	}
}
