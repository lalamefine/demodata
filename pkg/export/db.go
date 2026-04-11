package export

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/triboulin/demodata/pkg/ingest"
)

var validIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func ExportToDB(db *sql.DB, dataset ingest.Dataset, driver string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	tableNames := make([]string, 0, len(dataset))
	for tableName := range dataset {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	for _, tableName := range tableNames {
		if !validIdentifier.MatchString(tableName) {
			return fmt.Errorf("nom de table invalide: %s", tableName)
		}

		if _, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", tableName)); err != nil {
			return err
		}

		records := dataset[tableName]
		if len(records) == 0 {
			continue
		}

		cols := collectHeaders(records)
		insertSQL := buildInsertSQL(tableName, cols, driver)
		stmt, prepErr := tx.Prepare(insertSQL)
		if prepErr != nil {
			return prepErr
		}

		for _, rec := range records {
			args := make([]any, len(cols))
			for i, c := range cols {
				args[i] = rec[c]
			}
			if _, err = stmt.Exec(args...); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		if err = stmt.Close(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func buildInsertSQL(tableName string, cols []string, driver string) string {
	placeholders := make([]string, len(cols))
	for i := range cols {
		if normalizeDriver(driver) == "pgx" {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(cols, ","), strings.Join(placeholders, ","))
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
