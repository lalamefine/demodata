package transform

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strings"

	"github.com/lucasjones/reggen"
	"github.com/triboulin/demodata/pkg/config"
	"github.com/triboulin/demodata/pkg/ingest"
)

// Rule représente une transformation appliquée sur une ligne de données.
type Rule interface {
	Apply(r []ingest.Record) []ingest.Record
}

// Transformer applique un ensemble de règles sur un jeu de données.
type Transformer struct {
	Rules []Rule
}

// Apply applique l'ensemble des règles à chaque enregistrement.
func (t *Transformer) Apply(records []ingest.Record) []ingest.Record {
	for _, rule := range t.Rules {
		records = rule.Apply(records)
	}
	return records
}

// ApplyRules est une fonction utilitaire pour appliquer des règles à un jeu de données.
func ApplyRules(dataset ingest.Dataset, config *config.Config, seed int64) ingest.Dataset {
	if config == nil {
		return dataset
	}
	if dataset == nil {
		return ingest.Dataset{}
	}
	out := make(ingest.Dataset, len(dataset))
	for tableName, records := range dataset {
		copied := make([]ingest.Record, len(records))
		copy(copied, records)
		out[tableName] = copied
	}

	log.Printf("[transform] application des règles (seed=%d, %d table(s) configurée(s))", seed, len(config.Tables))
	for i, table := range config.Tables {
		tableName := table.Name
		if tableName == "" {
			tableName = ingest.DefaultTableName
		}
		records, ok := out[tableName]
		if !ok {
			continue
		}
		log.Printf("[transform] table %q : %d enregistrement(s), %d règle(s)", tableName, len(records), len(table.Transformers))

		r := rand.New(rand.NewSource(seed + int64(i)))
		transformer := &Transformer{}
		var shufflers []*Shuffler
		for _, tconf := range table.Transformers {
			var rule Rule
			switch strings.ToLower(tconf.Type) {
			case "none", "noop", "unchanged":
				continue
			case "masker":
				col, _ := tconf.Options["column_name"].(string)
				maskChar, ok := tconf.Options["mask_char"].(string)
				if !ok || maskChar == "" {
					maskChar = "*"
				}
				rule = &Masker{Column: col, MaskChar: maskChar}
			case "shuffler":
				cols := parseColumnNames(tconf.Options["column_names"])
				rule = &Shuffler{ColumnNames: cols, Rand: r}
				shufflers = append(shufflers, rule.(*Shuffler))
			case "generator":
				col, _ := tconf.Options["column_name"].(string)
				dataType, _ := tconf.Options["data_type"].(string)
				formatRegex, _ := tconf.Options["format"].(string)
				rule = &Generator{Column: col, DataType: strings.ToLower(dataType), Regex: formatRegex, Rand: r}
			default:
				continue
			}
			if rule != nil {
				if _, ok := rule.(*Shuffler); !ok {
					transformer.Rules = append(transformer.Rules, rule)
				}
			}
		}

		for _, s := range shufflers {
			records = ShuffleRecords(records, s.ColumnNames, s.Rand)
		}
		out[tableName] = transformer.Apply(records)
	}

	return out
}

// Masker remplace une valeur de colonne par un masque de type string.
type Masker struct {
	Column   string
	MaskChar string
}

func (m *Masker) ApplyOne(r ingest.Record) ingest.Record {
	val, ok := r[m.Column]
	if !ok || val == nil {
		return r
	}
	str := ""
	if s, ok2 := val.(string); ok2 {
		str = s
	} else {
		str = fmt.Sprint(val)
	}
	if len(str) == 0 {
		r[m.Column] = ""
		return r
	}
	mask := strings.Repeat(m.MaskChar, len(str))
	r[m.Column] = mask
	return r
}
func (m *Masker) Apply(records []ingest.Record) []ingest.Record {
	for i, r := range records {
		records[i] = m.ApplyOne(r)
	}
	return records
}

// Shuffler permute aléatoirement les valeurs du ou des colonnes indiquées sur l'ensemble des enregistrements.
type Shuffler struct {
	ColumnNames []string
	Rand        *rand.Rand
}

func (s *Shuffler) Apply(r []ingest.Record) []ingest.Record {
	return ShuffleRecords(r, s.ColumnNames, s.Rand)
}

// ShuffleRecords réorganise les valeurs des colonnes dans tous les enregistrements.
// cols est une liste de noms de colonnes à mélanger qui seront garantis de rester ensemble (ex: shuffle de "first_name,last_name" pour conserver la cohérence des noms).
func ShuffleRecords(records []ingest.Record, cols []string, r *rand.Rand) []ingest.Record {
	if len(cols) == 0 || len(records) == 0 {
		return records
	}

	// Construire des tuples de valeurs pour les colonnes demandées.
	values := make([][]any, len(records))
	for i, rec := range records {
		row := make([]any, len(cols))
		for ci, c := range cols {
			row[ci] = rec[c]
		}
		values[i] = row
	}

	r.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	// Réinjecter les tuples mélangés dans les enregistrements.
	for i, rec := range records {
		for ci, c := range cols {
			rec[c] = values[i][ci]
		}
	}
	return records
}

// Generator crée une valeur basée sur le type pour une colonne.
type Generator struct {
	Column   string
	DataType string
	Regex    string
	Rand     *rand.Rand
}

func (g *Generator) Apply(records []ingest.Record) []ingest.Record {
	for i, r := range records {
		records[i] = g.ApplyOne(r)
	}
	return records
}

func (g *Generator) ApplyOne(r ingest.Record) ingest.Record {
	value := g.generateValue()
	r[g.Column] = value
	return r
}

func (g *Generator) generateValue() any {
	if g.Regex != "" {
		val, err := generateFromRegex(g.Regex, g.Rand)
		if err == nil {
			return val
		}
		// if regex generation échoue, on continue sur le type
	}

	switch g.DataType {
	case "integer":
		if g.Rand != nil {
			return int64(g.Rand.Int63())
		}
		return int64(0)
	case "float":
		if g.Rand != nil {
			return g.Rand.Float64()
		}
		return float64(0)
	case "boolean":
		if g.Rand != nil {
			return g.Rand.Intn(2) == 1
		}
		return false
	default:
		if g.Rand != nil {
			return randomString(10, g.Rand)
		}
		return ""
	}
}

func generateFromRegex(pattern string, r *rand.Rand) (string, error) {
	if r == nil {
		r = rand.New(rand.NewSource(0))
	}

	val, err := reggen.Generate(pattern, 30)
	if err != nil {
		return "", err
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}

	if !re.MatchString(val) {
		return "", fmt.Errorf("generated value does not match regex")
	}

	return val, nil
}

func randomString(length int, r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	sb := strings.Builder{}
	for i := 0; i < length; i++ {
		sb.WriteByte(letters[r.Intn(len(letters))])
	}
	return sb.String()
}

func parseColumnNames(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return uniqueColumnNames(v)
	case []any:
		cols := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				cols = append(cols, s)
			}
		}
		return uniqueColumnNames(cols)
	case string:
		return uniqueColumnNames(strings.Split(v, ","))
	default:
		return nil
	}
}

// SampleDataset réduit chaque table du dataset à un sous-ensemble aléatoire reproductible.
// rate est compris entre 0.0 et 1.0 (1.0 = 100%, 0.5 = 50%).
// Si rate >= 1.0, le dataset est retourné sans modification.
func SampleDataset(dataset ingest.Dataset, rate float64, seed int64) ingest.Dataset {
	if rate >= 1.0 {
		return dataset
	}
	if rate <= 0 {
		rate = 0
	}

	// Ordre stable pour que le seed produise le même résultat.
	tableNames := make([]string, 0, len(dataset))
	for t := range dataset {
		tableNames = append(tableNames, t)
	}
	sort.Strings(tableNames)

	out := make(ingest.Dataset, len(dataset))
	for i, tableName := range tableNames {
		records := dataset[tableName]
		if len(records) == 0 {
			out[tableName] = records
			continue
		}
		n := int(math.Round(float64(len(records)) * rate))
		if n < 1 {
			n = 1
		}
		if n >= len(records) {
			out[tableName] = records
			continue
		}
		r := rand.New(rand.NewSource(seed + int64(i)))
		indices := r.Perm(len(records))[:n]
		sort.Ints(indices)
		sampled := make([]ingest.Record, n)
		for j, idx := range indices {
			sampled[j] = records[idx]
		}
		log.Printf("[transform] table %q : échantillon %d/%d enregistrement(s) (%.0f%%)", tableName, n, len(records), rate*100)
		out[tableName] = sampled
	}
	return out
}

// FilterFKViolations supprime des tables enfants les enregistrements dont la colonne FK
// ne référence plus une valeur présente dans la table parente (après échantillonnage).
// Les relations FK sont lues depuis le schéma BDD via ingest.GetForeignKeys.
func FilterFKViolations(dataset ingest.Dataset, relations []ingest.FKRelation) ingest.Dataset {
	for _, rel := range relations {
		parent, parentOK := dataset[rel.ParentTable]
		child, childOK := dataset[rel.ChildTable]
		if !parentOK || !childOK {
			continue
		}

		// Construire le set des valeurs valides dans la table parente.
		validKeys := make(map[string]struct{}, len(parent))
		for _, rec := range parent {
			if v, ok := rec[rel.ParentCol]; ok && v != nil {
				validKeys[fmt.Sprintf("%v", v)] = struct{}{}
			}
		}

		// Filtrer les lignes enfants qui référencent une valeur absente.
		before := len(child)
		filtered := child[:0:0] // slice vide, même backing array évité
		filtered = make([]ingest.Record, 0, len(child))
		for _, rec := range child {
			fkVal := fmt.Sprintf("%v", rec[rel.ChildCol])
			if _, ok := validKeys[fkVal]; ok {
				filtered = append(filtered, rec)
			}
		}
		removed := before - len(filtered)
		if removed > 0 {
			log.Printf("[transform] FK %s.%s → %s.%s : %d ligne(s) orpheline(s) supprimée(s) (%d restante(s))",
				rel.ChildTable, rel.ChildCol, rel.ParentTable, rel.ParentCol,
				removed, len(filtered))
		}
		dataset[rel.ChildTable] = filtered
	}
	return dataset
}

func uniqueColumnNames(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}
