package ingest

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

// Record représente une ligne / un objet de données homogène.
type Record map[string]interface{}

// LoadCSV ré-hydrate un flux CSV en une liste de Records.
// Cette fonction est un stub et doit être complétée pour gérer les dialectes CSV.
func LoadCSV(r io.Reader) ([]Record, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	// Lire l'en-tête
	headers, err := reader.Read()
	log.Printf("headers ignorés: %v", headers)
	if err != nil {
		return nil, err
	}

	records := make([]Record, 0)
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if len(row) == 0 {
			continue
		}
		// S'assurer que le nombre de colonnes correspond à l'en-tête
		if len(row) != len(headers) {
			// on ignore les colonnes en surplus ou manquantes
			min := len(headers)
			if len(row) < min {
				min = len(row)
			}
			rec := make(Record)
			for i := 0; i < min; i++ {
				rec[headers[i]] = parseStringValue(row[i])
			}
			records = append(records, rec)
			continue
		}

		rec := make(Record)
		for i, col := range headers {
			rec[col] = parseStringValue(row[i])
		}
		records = append(records, rec)
	}

	return records, nil
}

// LoadJSON charge une liste d'objets JSON depuis un flux.
func LoadJSON(r io.Reader) ([]Record, error) {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()

	var raw interface{}
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}

	arr, ok := raw.([]interface{})
	if !ok {
		return nil, &json.UnmarshalTypeError{Value: "non-array", Type: nil}
	}

	records := make([]Record, 0, len(arr))
	for _, item := range arr {
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		rec := make(Record)
		for k, v := range obj {
			rec[k] = normalizeJSONValue(v)
		}
		records = append(records, rec)
	}

	return records, nil
}

// LoadXLSX lit le premier onglet d'un fichier Excel (XLSX) et en extrait une liste de Record.
// La première ligne est interprétée comme l'en-tête des colonnes.
func LoadXLSX(r io.Reader) ([]Record, error) {
	f, err := excelize.OpenReader(r)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, nil
	}

	rows, err := f.GetRows(sheets[0])
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	headers := rows[0]
	log.Printf("headers ignorés: %v", headers)
	records := make([]Record, 0, len(rows)-1)

	for _, row := range rows[1:] {
		if len(row) == 0 {
			continue
		}
		min := len(headers)
		if len(row) < min {
			min = len(row)
		}
		rec := make(Record)
		for i := 0; i < min; i++ {
			rec[headers[i]] = parseStringValue(row[i])
		}
		records = append(records, rec)
	}

	return records, nil
}

// Load charge des données depuis un lecteur en fonction du format fourni.
// Les formats supportés sont : "csv", "json", "xlsx".
func Load(r io.Reader, format string) ([]Record, error) {
	switch format {
	case "csv":
		return LoadCSV(r)
	case "json":
		return LoadJSON(r)
	case "xlsx":
		return LoadXLSX(r)
	default:
		return nil, &ErrUnsupportedFormat{Format: format}
	}
}

// ErrUnsupportedFormat est retournée lorsque le format demandé n'est pas supporté.
type ErrUnsupportedFormat struct {
	Format string
}

func (e *ErrUnsupportedFormat) Error() string {
	return "format non supporté: " + e.Format
}

func parseStringValue(raw string) interface{} {
	if raw == "" {
		return ""
	}
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(raw); err == nil {
		return b
	}
	return raw
}

func normalizeJSONValue(v interface{}) interface{} {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case string, bool, float64, int, int64, nil:
		return t
	case []interface{}:
		arr := make([]interface{}, len(t))
		for i, v2 := range t {
			arr[i] = normalizeJSONValue(v2)
		}
		return arr
	case map[string]interface{}:
		m := make(map[string]interface{}, len(t))
		for k, v2 := range t {
			m[k] = normalizeJSONValue(v2)
		}
		return m
	default:
		return t
	}
}

func LoadFile(path string) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return Load(file, GetFileExtension(path))
}

func GetFileExtension(path string) string {
	if i := strings.LastIndex(path, "."); i != -1 && i+1 < len(path) {
		return strings.ToLower(path[i+1:])
	}
	return ""
}
