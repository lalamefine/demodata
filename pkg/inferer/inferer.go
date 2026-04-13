package inferer

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/triboulin/demodata/pkg/config"
	"github.com/triboulin/demodata/pkg/ingest"
)

// Patterns sémantiques pré-compilés — utilisés pour l'inférence sur les valeurs.
var (
	reEmail      = regexp.MustCompile(`(?i)^[^@\s]+@[^@\s]+\.[^@\s]+$`)
	rePhoneFR    = regexp.MustCompile(`^(\+33|0)[1-9][0-9]{8}$`)
	rePhoneIntl  = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
	reUUID       = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	reZipFR      = regexp.MustCompile(`^[0-9]{5}$`)
	reIBAN       = regexp.MustCompile(`^[A-Z]{2}[0-9]{2}[A-Z0-9]{1,30}$`)
	reDateISO    = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2})?`)
	reDateFR     = regexp.MustCompile(`^\d{2}/\d{2}/\d{4}`)
	reIP         = regexp.MustCompile(`^\d{1,3}(\.\d{1,3}){3}$`)
	reCreditCard = regexp.MustCompile(`^\d{16}$`)
	reAllDigits  = regexp.MustCompile(`^[0-9]+$`)
	reAllAlpha   = regexp.MustCompile(`^[A-Za-z]+$`)
)

// colSemanticHints mappe les sous-chaînes de noms de colonnes en sémantique explicite.
// L'ordre est intentionnel : les correspondances plus spécifiques sont placées en premier.
var colSemanticHints = []struct {
	keywords []string
	semantic string
}{
	{[]string{"email", "mail", "courriel"}, "email"},
	{[]string{"iban"}, "iban"},
	{[]string{"card_number", "carte", "creditcard", "credit_card", "pan"}, "credit_card"},
	{[]string{"phone", "tel", "telephone", "mobile", "portable", "fax"}, "phone"},
	{[]string{"ip_address", "ip_addr", "ipaddr", "ip"}, "ip"},
	{[]string{"uuid", "guid"}, "uuid"},
	{[]string{"zip", "postal", "code_postal", "postcode"}, "zip"},
	{[]string{"birthdate", "birth_date", "dob", "date_naissance", "naissance"}, "date"},
	{[]string{"date"}, "date"},
	{[]string{"first_name", "firstname", "prenom", "prénom", "given_name"}, "first_name"},
	{[]string{"last_name", "lastname", "nom", "surname", "family_name"}, "last_name"},
	{[]string{"full_name", "fullname", "name"}, "full_name"},
	{[]string{"address", "adresse", "street", "rue"}, "address"},
	{[]string{"city", "ville", "locality"}, "city"},
	{[]string{"country", "pays", "nation"}, "country"},
	{[]string{"password", "passwd", "pwd", "secret", "token", "hash", "key"}, "secret"},
	{[]string{"ssn", "social_security", "nss", "numero_secu", "secu"}, "ssn"},
}

// columnSemantic détecte le type sémantique d'une colonne à partir de son nom et d'un échantillon de valeurs.
// L'échantillon est limité à quelques valeurs — on ne stocke pas la liste complète.
func columnSemantic(colName string, sample []string) string {
	lower := strings.ToLower(colName)

	// 1. Correspondance par nom de colonne (prioritaire).
	for _, hint := range colSemanticHints {
		for _, kw := range hint.keywords {
			if strings.Contains(lower, kw) {
				return hint.semantic
			}
		}
	}

	// 2. Correspondance par pattern sur l'échantillon.
	if len(sample) == 0 {
		return ""
	}
	matches := func(re *regexp.Regexp) bool {
		matched := 0
		for _, v := range sample {
			if re.MatchString(v) {
				matched++
			}
		}
		return matched >= len(sample)/2+1 // majorité simple
	}

	switch {
	case matches(reEmail):
		return "email"
	case matches(reUUID):
		return "uuid"
	case matches(reIBAN):
		return "iban"
	case matches(rePhoneFR):
		return "phone"
	case matches(rePhoneIntl):
		return "phone"
	case matches(reCreditCard):
		return "credit_card"
	case matches(reDateISO):
		return "date"
	case matches(reDateFR):
		return "date"
	case matches(reIP):
		return "ip"
	case strings.Contains(lower, "zip") && matches(reZipFR):
		return "zip"
	}
	return ""
}

// colScanResult résume les caractéristiques structurelles d'une colonne.
type colScanResult struct {
	count     int
	minLen    int
	maxLen    int
	allDigits bool
	allAlpha  bool
	semantic  string
	sample    []string // petit échantillon (≤ sampleSize valeurs) — jamais stocké en config
	// statistiques numériques
	numMin, numMax, numAvg, numStd float64
	isNumeric                      bool
}

const sampleSize = 20 // taille maximale de l'échantillon pour la détection sémantique

// scanColumn parcourt la colonne en une seule passe sans stocker toutes les valeurs distinctes.
func scanColumn(records []ingest.Record, col string, colType config.DataType) colScanResult {
	res := colScanResult{minLen: 1<<31 - 1, allDigits: true, allAlpha: true}

	var nums []float64

	for _, r := range records {
		v, ok := r[col]
		if !ok || v == nil {
			continue
		}
		s := fmt.Sprint(v)
		l := len(s)
		res.count++
		if l < res.minLen {
			res.minLen = l
		}
		if l > res.maxLen {
			res.maxLen = l
		}
		if res.allDigits && !reAllDigits.MatchString(s) {
			res.allDigits = false
		}
		if res.allAlpha && !reAllAlpha.MatchString(s) {
			res.allAlpha = false
		}
		if len(res.sample) < sampleSize {
			res.sample = append(res.sample, s)
		}
		if colType == config.Integer || colType == config.Float {
			var f float64
			switch x := v.(type) {
			case int:
				f = float64(x)
			case int8:
				f = float64(x)
			case int16:
				f = float64(x)
			case int32:
				f = float64(x)
			case int64:
				f = float64(x)
			case float32:
				f = float64(x)
			case float64:
				f = x
			default:
				continue
			}
			nums = append(nums, f)
			res.isNumeric = true
		}
	}
	if res.count == 0 {
		res.minLen = 0
	}
	res.numMin, res.numMax, res.numAvg, res.numStd = computeNumStats(nums)
	res.semantic = columnSemantic(col, res.sample)
	return res
}

func computeNumStats(nums []float64) (min, max, avg, std float64) {
	if len(nums) == 0 {
		return 0, 0, 0, 0
	}
	min, max = nums[0], nums[0]
	for _, n := range nums {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
		avg += n
	}
	avg /= float64(len(nums))
	for _, n := range nums {
		std += (n - avg) * (n - avg)
	}
	if len(nums) > 1 {
		std = std / float64(len(nums)-1)
	} else {
		std = 0
	}
	return min, max, avg, std
}

// inferRegexFromScan déduit un pattern regex structurel à partir des statistiques de scan.
// Ne contient JAMAIS les valeurs originales.
func inferRegexFromScan(stats colScanResult) string {
	minL, maxL := stats.minLen, stats.maxLen

	if stats.allDigits && stats.count > 0 {
		if minL == maxL {
			return fmt.Sprintf(`^[0-9]{%d}$`, minL)
		}
		return fmt.Sprintf(`^[0-9]{%d,%d}$`, minL, maxL)
	}
	if stats.allAlpha && stats.count > 0 {
		if minL == maxL {
			return fmt.Sprintf(`^[A-Za-z]{%d}$`, minL)
		}
		return fmt.Sprintf(`^[A-Za-z]{%d,%d}$`, minL, maxL)
	}
	if maxL == 0 {
		return `.+`
	}
	if minL == maxL {
		return fmt.Sprintf(`.{%d}`, minL)
	}
	return fmt.Sprintf(`.{%d,%d}`, minL, maxL)
}

// InferRuleSet construit une config à partir d'un jeu d'enregistrements.
// Les valeurs originales ne sont jamais incluses dans les options des règles générées.
// colOrderHint, si fourni, indique l'ordre des colonnes par table (sinon tri alphabétique).
func InferRuleSet(dataset ingest.Dataset, colOrderHint map[string][]string) *config.Config {
	if len(dataset) == 0 {
		return &config.Config{Tables: nil}
	}

	tableNames := make([]string, 0, len(dataset))
	for tableName := range dataset {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	tables := make([]config.TableConfig, 0, len(tableNames))
	for _, tableName := range tableNames {
		records := dataset[tableName]
		if len(records) == 0 {
			tables = append(tables, config.TableConfig{Name: tableName, Transformers: []config.TransformerConfig{}})
			continue
		}

		colTypes := detectColumns(records)

		// Trier les colonnes pour une sortie déterministe.
		colNames := make([]string, 0, len(colTypes))
		for cn := range colTypes {
			colNames = append(colNames, cn)
		}
		if colOrderHint != nil {
			if hint, ok := colOrderHint[tableName]; ok && len(hint) == len(colNames) {
				colNames = hint
			} else {
				sort.Strings(colNames)
			}
		} else {
			sort.Strings(colNames)
		}

		table := config.TableConfig{Name: tableName, Transformers: []config.TransformerConfig{}}
		for _, col := range colNames {
			colType := colTypes[col]
			stats := scanColumn(records, col, colType)
			if stats.count == 0 {
				continue
			}

			opts := map[string]any{
				"column_name": col,
				"format":      inferRegexFromScan(stats),
			}
			if stats.semantic != "" {
				opts["semantic"] = stats.semantic
			}
			if stats.isNumeric {
				opts["distribution"] = map[string]any{
					"min": stats.numMin,
					"max": stats.numMax,
					"avg": stats.numAvg,
					"std": stats.numStd,
				}
			}

			rule := config.TransformerConfig{
				Type:    "none",
				Name:    fmt.Sprintf("%s - none", col),
				Options: opts,
			}
			table.Transformers = append(table.Transformers, rule)
		}

		tables = append(tables, table)
	}

	return &config.Config{Tables: tables}
}

func detectColumns(records []ingest.Record) map[string]config.DataType {
	cols := map[string]config.DataType{}
	for _, rec := range records {
		for k, v := range rec {
			if existing, ok := cols[k]; ok && existing != config.String {
				cols[k] = commonType(existing, inferType(v))
			} else {
				cols[k] = inferType(v)
			}
		}
	}
	return cols
}

func inferType(v interface{}) config.DataType {
	if v == nil {
		return config.String
	}
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return config.Integer
	case float32, float64:
		return config.Float
	case bool:
		return config.Boolean
	default:
		return config.String
	}
}

func commonType(a, b config.DataType) config.DataType {
	if a == b {
		return a
	}
	return config.String
}
