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

	headers := collectHeaders(records)

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

		headers := collectHeaders(records)
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

func collectHeaders(records []ingest.Record) []string {
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
func ExportToFile(dataset ingest.Dataset, path, format string) error {
	var exporter Exporter
	switch format {
	case "csv":
		exporter = &CSVExporter{DestinationPath: path}
	case "json":
		exporter = &JSONExporter{DestinationPath: path}
	case "xlsx":
		exporter = &XLSXExporter{DestinationPath: path}
	default:
		return fmt.Errorf("format non supporté: %s", format)
	}

	b, err := exporter.Export(dataset)
	if err != nil {
		return err
	}

	return os.WriteFile(path, b, 0o644)
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
