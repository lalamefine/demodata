package export

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/triboulin/demodata/pkg/ingest"
	"github.com/xuri/excelize/v2"
)

// Exporter gère l'export des données vers différents formats.
type Exporter interface {
	Export(dataset ingest.Dataset) ([]byte, error)
	Destination() string
}

// =========== CSVExporter ============
type CSVExporter struct {
	DestinationPath string
}

func (e *CSVExporter) Destination() string {
	return e.DestinationPath
}
func (e *CSVExporter) Export(dataset ingest.Dataset) ([]byte, error) {
	records := firstTableRecords(dataset)
	if len(records) == 0 {
		return []byte{}, nil
	}

	headers := collectHeaders(records, nil)

	buf := &bytes.Buffer{}
	w := csv.NewWriter(buf)
	if err := w.Write(headers); err != nil {
		return nil, err
	}

	for _, rec := range records {
		row := make([]string, len(headers))
		for i, h := range headers {
			row[i] = valueToString(rec[h])
		}
		if err := w.Write(row); err != nil {
			return nil, err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// JSONExporter sérialise les records sous forme de JSON indenté.
type JSONExporter struct {
	DestinationPath string
}

func (e *JSONExporter) Destination() string {
	return e.DestinationPath
}

func (e *JSONExporter) Export(dataset ingest.Dataset) ([]byte, error) {
	records := firstTableRecords(dataset)
	return json.MarshalIndent(records, "", "  ")
}

// XLSXExporter exporte les records dans un fichier Excel (XLSX) en écrivant sur la feuille "Sheet1".
type XLSXExporter struct {
	DestinationPath string
	ColOrder        map[string][]string // ordre des colonnes par table/feuille (optionnel)
}

func (e *XLSXExporter) Destination() string {
	return e.DestinationPath
}

func (e *XLSXExporter) Export(dataset ingest.Dataset) ([]byte, error) {
	file := excelize.NewFile()

	defaultSheet := file.GetSheetName(file.GetActiveSheetIndex())
	if defaultSheet == "" {
		defaultSheet = "Sheet1"
	}
	if len(dataset) == 0 {
		return []byte{}, nil
	}

	sheetNames := sortedTableNames(dataset)
	for i, sheet := range sheetNames {
		records := dataset[sheet]
		targetSheet := sheet
		if targetSheet == "" {
			targetSheet = fmt.Sprintf("Sheet%d", i+1)
		}
		if i == 0 {
			file.SetSheetName(defaultSheet, targetSheet)
		} else {
			file.NewSheet(targetSheet)
		}

		headers := collectHeaders(records, e.ColOrder[sheet])
		for ci, h := range headers {
			cell, _ := excelize.CoordinatesToCellName(ci+1, 1)
			file.SetCellValue(targetSheet, cell, h)
		}

		for ri, rec := range records {
			for ci, h := range headers {
				cell, _ := excelize.CoordinatesToCellName(ci+1, ri+2)
				file.SetCellValue(targetSheet, cell, rec[h])
			}
		}
	}

	file.SetActiveSheet(0)

	buf := &bytes.Buffer{}
	if err := file.Write(buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func collectHeaders(records []ingest.Record, hint []string) []string {
	if len(hint) > 0 {
		return hint
	}
	set := make(map[string]struct{})
	for _, rec := range records {
		for k := range rec {
			set[k] = struct{}{}
		}
	}

	headers := make([]string, 0, len(set))
	for k := range set {
		headers = append(headers, k)
	}
	sort.Strings(headers)
	return headers
}

// encodeOrderedJSON sérialise les records en JSON en préservant l'ordre des colonnes donné par order.
// Si order est vide, les clés sont triées alphabétiquement (comportement standard).
func encodeOrderedJSON(records []ingest.Record, order []string) ([]byte, error) {
	if len(order) == 0 {
		return json.MarshalIndent(records, "", "  ")
	}

	buf := &bytes.Buffer{}
	buf.WriteString("[")
	for i, rec := range records {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("\n  {")
		for j, k := range order {
			if j > 0 {
				buf.WriteString(",")
			}
			key, _ := json.Marshal(k)
			val, _ := json.Marshal(rec[k])
			buf.WriteString("\n    ")
			buf.Write(key)
			buf.WriteString(": ")
			buf.Write(val)
		}
		buf.WriteString("\n  }")
	}
	buf.WriteString("\n]")
	return buf.Bytes(), nil
}

func valueToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprint(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// ExportToFile enregistre le résultat de l'export dans un fichier.
// Le format doit être l'un de : "csv", "json", "xlsx".
// colOrder permet de préserver l'ordre des colonnes par table (peut être nil).
// Pour CSV et JSON avec plusieurs tables, un fichier est créé par table avec le suffixe _tablename.
func ExportToFile(dataset ingest.Dataset, outputPath, format string, colOrder map[string][]string) error {
	switch format {
	case "csv":
		return exportPerTable(dataset, outputPath, format, func(tableName string, records []ingest.Record) ([]byte, error) {
			headers := collectHeaders(records, colOrder[tableName])
			buf := &bytes.Buffer{}
			w := csv.NewWriter(buf)
			if err := w.Write(headers); err != nil {
				return nil, err
			}
			for _, rec := range records {
				row := make([]string, len(headers))
				for i, h := range headers {
					row[i] = valueToString(rec[h])
				}
				if err := w.Write(row); err != nil {
					return nil, err
				}
			}
			w.Flush()
			return buf.Bytes(), w.Error()
		})
	case "json":
		return exportPerTable(dataset, outputPath, format, func(tableName string, records []ingest.Record) ([]byte, error) {
			return encodeOrderedJSON(records, colOrder[tableName])
		})
	case "xlsx":
		b, err := (&XLSXExporter{DestinationPath: outputPath, ColOrder: colOrder}).Export(dataset)
		if err != nil {
			return err
		}
		return os.WriteFile(outputPath, b, 0o644)
	default:
		return fmt.Errorf("format non supporté: %s", format)
	}
}

// exportPerTable écrit un fichier par table. Si le dataset n'a qu'une seule table,
// le fichier est écrit directement à outputPath. Sinon, chaque table est écrite dans
// <base>_<tablename>.<ext>.
func exportPerTable(dataset ingest.Dataset, outputPath, ext string, serialize func(tableName string, records []ingest.Record) ([]byte, error)) error {
	tables := sortedTableNames(dataset)
	if len(tables) == 1 {
		b, err := serialize(tables[0], dataset[tables[0]])
		if err != nil {
			return err
		}
		return os.WriteFile(outputPath, b, 0o644)
	}

	dotExt := "." + ext
	base := outputPath
	if idx := len(outputPath) - len(dotExt); idx > 0 && outputPath[idx:] == dotExt {
		base = outputPath[:idx]
	}

	for _, table := range tables {
		b, err := serialize(table, dataset[table])
		if err != nil {
			return fmt.Errorf("table %s: %w", table, err)
		}
		filePath := base + "_" + table + dotExt
		if err := os.WriteFile(filePath, b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func firstTableRecords(dataset ingest.Dataset) []ingest.Record {
	if len(dataset) == 0 {
		return nil
	}
	names := sortedTableNames(dataset)
	return dataset[names[0]]
}

func sortedTableNames(dataset ingest.Dataset) []string {
	names := make([]string, 0, len(dataset))
	for name := range dataset {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
