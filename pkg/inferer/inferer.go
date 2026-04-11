package inferer

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/triboulin/demodata/pkg/config"
	"github.com/triboulin/demodata/pkg/ingest"
)

var (
	reEmail   = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
	reUUID    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	reZipCode = regexp.MustCompile(`^[0-9]{5}(-[0-9]{4})?$`)
)

// InferRuleSet construit une config à partir d'un jeu d'enregistrements.
func InferRuleSet(dataset ingest.Dataset) *config.Config {
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

		cols := detectColumns(records)
		table := config.TableConfig{Name: tableName, Transformers: []config.TransformerConfig{}}
		for col, st := range cols {
			values := collectDistinct(records, col)
			if len(values) == 0 {
				continue
			}

			regex := inferRegex(col, values)
			rule := config.TransformerConfig{
				Type: "none",
				Name: fmt.Sprintf("%s - none", col),
				Options: map[string]any{
					"column_name": col,
					"values":      values,
					"format":      regex,
				},
			}

			if st == config.Integer || st == config.Float {
				min, max, avg, std := numericStats(values)
				rule.Options["distribution"] = map[string]any{"min": min, "max": max, "avg": avg, "std": std}
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

func collectDistinct(records []ingest.Record, col string) []any {
	set := map[interface{}]struct{}{}
	for _, r := range records {
		if v, ok := r[col]; ok {
			set[v] = struct{}{}
		}
	}

	values := make([]any, 0, len(set))
	for v := range set {
		values = append(values, v)
	}

	return values
}

func numericStats(values []any) (min, max, avg, std float64) {
	var nums []float64
	for _, v := range values {
		switch x := v.(type) {
		case int:
			nums = append(nums, float64(x))
		case int8:
			nums = append(nums, float64(x))
		case int16:
			nums = append(nums, float64(x))
		case int32:
			nums = append(nums, float64(x))
		case int64:
			nums = append(nums, float64(x))
		case float32:
			nums = append(nums, float64(x))
		case float64:
			nums = append(nums, x)
		}
	}
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
	// string l'emporte
	return config.String
}

func inferRegex(col string, values []any) string {
	colLower := strings.ToLower(col)
	if strings.Contains(colLower, "email") {
		return `^[^@\s]+@[^@\s]+\.[^@\s]+$`
	}
	if strings.Contains(colLower, "zip") {
		return `^[0-9]{5}(-[0-9]{4})?$`
	}
	if strings.Contains(colLower, "uuid") {
		return `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`
	}

	if len(values) == 0 {
		return ""
	}

	if allDigits(values) {
		minL, maxL := minMaxLength(values)
		if minL == maxL {
			return fmt.Sprintf(`^[0-9]{%d}$`, minL)
		}
		return fmt.Sprintf(`^[0-9]{%d,%d}$`, minL, maxL)
	}

	if allAlpha(values) {
		minL, maxL := minMaxLength(values)
		if minL == maxL {
			return fmt.Sprintf(`^[A-Za-z]{%d}$`, minL)
		}
		return fmt.Sprintf(`^[A-Za-z]{%d,%d}$`, minL, maxL)
	}

	if len(values) <= 10 {
		sort.Slice(values, func(i, j int) bool { return fmt.Sprint(values[i]) < fmt.Sprint(values[j]) })
		parts := make([]string, 0, len(values))
		for _, v := range values {
			parts = append(parts, regexp.QuoteMeta(fmt.Sprint(v)))
		}
		return `^(?:` + strings.Join(parts, "|") + `)$`
	}

	minL, maxL := minMaxLength(values)
	return fmt.Sprintf(`^.{%d,%d}$`, minL, maxL)
}

func allDigits(values []any) bool {
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			return false
		}
		if !regexp.MustCompile(`^[0-9]+$`).MatchString(s) {
			return false
		}
	}
	return true
}

func allAlpha(values []any) bool {
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			return false
		}
		if !regexp.MustCompile(`^[A-Za-z]+$`).MatchString(s) {
			return false
		}
	}
	return true
}

func minMaxLength(values []any) (int, int) {
	min, max := 999999, 0
	for _, v := range values {
		l := len(fmt.Sprint(v))
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
	}
	if min == 999999 {
		min = 0
	}
	return min, max
}

func isMonotonicInt(values []string) bool {
	p := regexp.MustCompile(`^[0-9]+$`)
	for _, v := range values {
		if !p.MatchString(v) {
			return false
		}
	}
	return true
}
