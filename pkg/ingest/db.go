package ingest

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"sort"
)

var validIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func OpenDB(driver, dsn string) (*sql.DB, error) {
	normalized := normalizeDriver(driver)
	log.Printf("[ingest] connexion BDD driver=%s", normalized)
	db, err := sql.Open(normalized, dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	log.Printf("[ingest] connexion BDD établie")
	return db, nil
}

func ListTables(db *sql.DB, driver string) ([]string, error) {
	q, err := listTablesQuery(driver)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(tables)
	return tables, nil
}

func LoadDB(db *sql.DB, driver string, tableNames []string) (Dataset, error) {
	if len(tableNames) == 0 {
		var err error
		tableNames, err = ListTables(db, driver)
		if err != nil {
			return nil, err
		}
	}

	log.Printf("[ingest] chargement BDD : %d table(s) : %v", len(tableNames), tableNames)
	dataset := make(Dataset, len(tableNames))
	for _, tableName := range tableNames {
		records, err := loadTable(db, tableName)
		if err != nil {
			return nil, err
		}
		log.Printf("[ingest] table %q : %d enregistrement(s) chargé(s)", tableName, len(records))
		dataset[tableName] = records
	}
	log.Printf("[ingest] chargement BDD terminé")
	return dataset, nil
}

func loadTable(db *sql.DB, tableName string) ([]Record, error) {
	if !validIdentifier.MatchString(tableName) {
		return nil, fmt.Errorf("nom de table invalide: %s", tableName)
	}
	q := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	records := make([]Record, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		rec := make(Record, len(columns))
		for i, col := range columns {
			rec[col] = normalizeDBValue(values[i])
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func normalizeDriver(driver string) string {
	switch driver {
	case "postgres", "pgsql", "pgx":
		return "pgx"
	case "sqlite", "sqlite3":
		return "sqlite"
	default:
		return driver
	}
}

func listTablesQuery(driver string) (string, error) {
	switch normalizeDriver(driver) {
	case "sqlite":
		return "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'", nil
	case "mysql":
		return "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()", nil
	case "pgx":
		return "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'", nil
	default:
		return "", fmt.Errorf("driver non supporte: %s", driver)
	}
}

func normalizeDBValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}

// FKRelation décrit une contrainte de clé étrangère entre deux tables.
type FKRelation struct {
	ChildTable  string
	ChildCol    string
	ParentTable string
	ParentCol   string
}

// GetForeignKeys interroge le schéma de la BDD et retourne toutes les relations FK connues.
// Supporte MySQL, PostgreSQL (pgx) et SQLite.
func GetForeignKeys(db *sql.DB, driver string, tableNames []string) ([]FKRelation, error) {
	switch normalizeDriver(driver) {
	case "mysql":
		return getForeignKeysMySQL(db)
	case "pgx":
		return getForeignKeysPG(db)
	case "sqlite":
		return getForeignKeysSQLite(db, tableNames)
	default:
		return nil, nil
	}
}

func getForeignKeysMySQL(db *sql.DB) ([]FKRelation, error) {
	q := `
		SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE REFERENCED_TABLE_NAME IS NOT NULL
		  AND TABLE_SCHEMA = DATABASE()`
	rows, err := db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("lecture FK MySQL: %w", err)
	}
	defer rows.Close()
	return scanFKRows(rows)
}

func getForeignKeysPG(db *sql.DB) ([]FKRelation, error) {
	q := `
		SELECT tc.table_name, kcu.column_name, ccu.table_name, ccu.column_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
		  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
		  ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema = 'public'`
	rows, err := db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("lecture FK PostgreSQL: %w", err)
	}
	defer rows.Close()
	return scanFKRows(rows)
}

func getForeignKeysSQLite(db *sql.DB, tableNames []string) ([]FKRelation, error) {
	var rels []FKRelation
	for _, tbl := range tableNames {
		if !validIdentifier.MatchString(tbl) {
			continue
		}
		rows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", tbl))
		if err != nil {
			return nil, fmt.Errorf("PRAGMA FK SQLite (%s): %w", tbl, err)
		}
		for rows.Next() {
			// id, seq, table, from, to, on_update, on_delete, match
			var id, seq int
			var parentTable, fromCol, toCol, onUpdate, onDelete, match string
			if err := rows.Scan(&id, &seq, &parentTable, &fromCol, &toCol, &onUpdate, &onDelete, &match); err != nil {
				rows.Close()
				return nil, err
			}
			if toCol == "" {
				toCol = "id"
			}
			rels = append(rels, FKRelation{
				ChildTable:  tbl,
				ChildCol:    fromCol,
				ParentTable: parentTable,
				ParentCol:   toCol,
			})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return rels, nil
}

func scanFKRows(rows *sql.Rows) ([]FKRelation, error) {
	var rels []FKRelation
	for rows.Next() {
		var r FKRelation
		if err := rows.Scan(&r.ChildTable, &r.ChildCol, &r.ParentTable, &r.ParentCol); err != nil {
			return nil, err
		}
		rels = append(rels, r)
	}
	return rels, rows.Err()
}
