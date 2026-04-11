package ingest

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
)

var validIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func OpenDB(driver, dsn string) (*sql.DB, error) {
	normalized := normalizeDriver(driver)
	db, err := sql.Open(normalized, dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
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

	dataset := make(Dataset, len(tableNames))
	for _, tableName := range tableNames {
		records, err := loadTable(db, tableName)
		if err != nil {
			return nil, err
		}
		dataset[tableName] = records
	}
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
