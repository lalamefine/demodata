package export

import (
	"database/sql"
	"fmt"
	"log"
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

	// Désactiver les contraintes FK le temps de la transaction
	// MySQL : SET FOREIGN_KEY_CHECKS=0 ; PostgreSQL : SET CONSTRAINTS ALL DEFERRED
	switch normalizeDriver(driver) {
	case "mysql":
		if _, err = tx.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
			return fmt.Errorf("désactivation FK mysql: %w", err)
		}
		log.Printf("[export] FK MySQL désactivées pour la transaction")
	case "pgx":
		if _, err = tx.Exec("SET CONSTRAINTS ALL DEFERRED"); err != nil {
			return fmt.Errorf("report contraintes pg: %w", err)
		}
		log.Printf("[export] contraintes PostgreSQL différées pour la transaction")
	}

	// Phase 1 : DELETE toutes les tables
	for _, tableName := range tableNames {
		if !validIdentifier.MatchString(tableName) {
			err = fmt.Errorf("nom de table invalide: %s", tableName)
			return err
		}
		var result sql.Result
		result, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
		if err != nil {
			return fmt.Errorf("delete %s: %w", tableName, err)
		}
		deleted, _ := result.RowsAffected()
		log.Printf("[export] table %q : %d ligne(s) supprimée(s)", tableName, deleted)
	}

	// Phase 2 : INSERT toutes les tables
	for _, tableName := range tableNames {
		records := dataset[tableName]
		if len(records) == 0 {
			continue
		}

		cols := collectHeaders(records, nil)
		insertSQL := buildInsertSQL(tableName, cols, driver)
		stmt, prepErr := tx.Prepare(insertSQL)
		if prepErr != nil {
			return prepErr
		}
		defer stmt.Close()

		for _, rec := range records {
			args := make([]any, len(cols))
			for i, c := range cols {
				args[i] = rec[c]
			}
			if _, err = stmt.Exec(args...); err != nil {
				return err
			}
		}
		log.Printf("[export] table %q : %d ligne(s) insérée(s)", tableName, len(records))
	}

	// Réactiver les FKs MySQL avant le commit
	if normalizeDriver(driver) == "mysql" {
		if _, err = tx.Exec("SET FOREIGN_KEY_CHECKS=1"); err != nil {
			return fmt.Errorf("réactivation FK mysql: %w", err)
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
