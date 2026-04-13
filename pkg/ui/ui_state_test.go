package ui

import (
	"testing"

	"github.com/triboulin/demodata/pkg/config"
)

// makeTestState construit un uiState minimal pour les tests de transitions d'état.
func makeTestState(transformers []config.TransformerConfig) *uiState {
	return &uiState{
		selectedTable: "test",
		selectedIndex: 0,
		ruleConfig: &config.Config{
			Tables: []config.TableConfig{
				{Name: "test", Transformers: transformers},
			},
		},
	}
}

// TestApplyShufflerColumnsAction_MergeTwo vérifie que 2 règles mono-colonne fusionnent en
// une règle shuffler quand elles sont sélectionnées ensemble.
func TestApplyShufflerColumnsAction_MergeTwo(t *testing.T) {
	state := makeTestState([]config.TransformerConfig{
		{Name: "none", Type: "none", Options: map[string]any{"column_name": "name"}},
		{Name: "none", Type: "none", Options: map[string]any{"column_name": "email"}},
	})
	tableCfg := tableConfigForSelected(state)
	applyShufflerColumnsAction(state, tableCfg, 0, []string{"name", "email"})

	if len(tableCfg.Transformers) != 1 {
		t.Fatalf("expected 1 transformer after merge, got %d", len(tableCfg.Transformers))
	}
	tr := tableCfg.Transformers[0]
	if tr.Type != "shuffler" {
		t.Errorf("expected shuffler type, got %q", tr.Type)
	}
	cols := parseShufflerColumns(tr.Options["column_names"])
	if len(cols) != 2 {
		t.Errorf("expected 2 columns in shuffler, got %d: %v", len(cols), cols)
	}
	if state.selectedIndex != 0 {
		t.Errorf("expected selectedIndex 0, got %d", state.selectedIndex)
	}
}

// TestApplyShufflerColumnsAction_DissolveToMono vérifie que décocher jusqu'à 1 colonne
// dissout le shuffler et recrée les règles mono-colonne.
func TestApplyShufflerColumnsAction_DissolveToMono(t *testing.T) {
	state := makeTestState([]config.TransformerConfig{
		{Name: "shuffler", Type: "shuffler", Options: map[string]any{
			"column_names": []string{"name", "email"},
		}},
		{Name: "none", Type: "none", Options: map[string]any{"column_name": "age"}},
	})
	tableCfg := tableConfigForSelected(state)
	applyShufflerColumnsAction(state, tableCfg, 0, []string{"name"})

	if len(tableCfg.Transformers) != 3 {
		t.Fatalf("expected 3 transformers after dissolve, got %d", len(tableCfg.Transformers))
	}
	// Index 0 : "name" conserve le type shuffler (solo).
	if tableCfg.Transformers[0].Type != "shuffler" {
		t.Errorf("index 0 should keep type shuffler, got %q", tableCfg.Transformers[0].Type)
	}
	cols := parseShufflerColumns(tableCfg.Transformers[0].Options["column_names"])
	if len(cols) != 1 || cols[0] != "name" {
		t.Errorf("index 0 column_names should be [name], got %v", cols)
	}
	// Index 1 : "age" reste en place.
	if col, _ := tableCfg.Transformers[1].Options["column_name"].(string); col != "age" {
		t.Errorf("index 1 should stay 'age', got %q", col)
	}
	// Index 2 : "email" appendé en fin, type none.
	if tableCfg.Transformers[2].Type != "none" {
		t.Errorf("index 2 should be type none, got %q", tableCfg.Transformers[2].Type)
	}
	if col, _ := tableCfg.Transformers[2].Options["column_name"].(string); col != "email" {
		t.Errorf("index 2 should be 'email', got %q", col)
	}
}

// TestApplyShufflerColumnsAction_DissolveEmpty vérifie la dissolution quand aucune colonne
// n'est sélectionnée : la 1re col restaurée garde le type shuffler, les suivantes sont none.
func TestApplyShufflerColumnsAction_DissolveEmpty(t *testing.T) {
	state := makeTestState([]config.TransformerConfig{
		{Name: "shuffler", Type: "shuffler", Options: map[string]any{
			"column_names": []string{"name", "email"},
		}},
	})
	tableCfg := tableConfigForSelected(state)
	applyShufflerColumnsAction(state, tableCfg, 0, []string{})

	if len(tableCfg.Transformers) != 2 {
		t.Fatalf("expected 2 transformers after full dissolve, got %d", len(tableCfg.Transformers))
	}
	// La 1re colonne ("name") conserve le type shuffler.
	if tableCfg.Transformers[0].Type != "shuffler" {
		t.Errorf("index 0 should keep type shuffler, got %q", tableCfg.Transformers[0].Type)
	}
	// La 2e colonne ("email") devient none.
	if tableCfg.Transformers[1].Type != "none" {
		t.Errorf("index 1 should be type none, got %q", tableCfg.Transformers[1].Type)
	}
}

// TestApplyShufflerColumnsAction_PartialUncheck vérifie que décocher une colonne d'un
// groupe de 3 recrée la colonne décochée en mono-colonne tout en maintenant le shuffler.
// La colonne décochée est appendée en fin de liste (les items sous le shuffler ne décalent pas).
func TestApplyShufflerColumnsAction_PartialUncheck(t *testing.T) {
	state := makeTestState([]config.TransformerConfig{
		{Name: "shuffler", Type: "shuffler", Options: map[string]any{
			"column_names": []string{"name", "email", "phone"},
		}},
		{Name: "none", Type: "none", Options: map[string]any{"column_name": "age"}},
	})
	tableCfg := tableConfigForSelected(state)
	// Décocher "phone" → le shuffler garde name+email, phone redevient mono-colonne.
	applyShufflerColumnsAction(state, tableCfg, 0, []string{"name", "email"})

	// Attendu : 1 shuffler (name+email) + 1 mono "age" + 1 mono "phone" (fin) = 3 total.
	if len(tableCfg.Transformers) != 3 {
		t.Fatalf("expected 3 transformers, got %d", len(tableCfg.Transformers))
	}
	// Shuffler reste à l'index 0.
	if tableCfg.Transformers[0].Type != "shuffler" {
		t.Errorf("index 0 should still be shuffler, got %q", tableCfg.Transformers[0].Type)
	}
	cols := parseShufflerColumns(tableCfg.Transformers[0].Options["column_names"])
	if len(cols) != 2 {
		t.Errorf("shuffler should have 2 columns, got %d: %v", len(cols), cols)
	}
	// "age" reste à l'index 1 (ne décale pas).
	if col, _ := tableCfg.Transformers[1].Options["column_name"].(string); col != "age" {
		t.Errorf("index 1 should stay 'age', got %q", col)
	}
	// "phone" appendé en dernier.
	if col, _ := tableCfg.Transformers[2].Options["column_name"].(string); col != "phone" {
		t.Errorf("index 2 should be 'phone' (appended), got %q", col)
	}
}

// TestApplyShufflerColumnsAction_AbsorbExistingMono vérifie que cocher une colonne déjà
// présente comme règle mono-colonne l'absorbe dans le shuffler (pas de doublon).
func TestApplyShufflerColumnsAction_AbsorbExistingMono(t *testing.T) {
	state := makeTestState([]config.TransformerConfig{
		{Name: "none", Type: "none", Options: map[string]any{"column_name": "name"}},
		{Name: "none", Type: "none", Options: map[string]any{"column_name": "email"}},
		{Name: "none", Type: "none", Options: map[string]any{"column_name": "phone"}},
	})
	tableCfg := tableConfigForSelected(state)
	applyShufflerColumnsAction(state, tableCfg, 0, []string{"name", "email"})

	// "phone" doit rester seul, name+email fusionnent en shuffler → 2 règles au total.
	if len(tableCfg.Transformers) != 2 {
		t.Fatalf("expected 2 transformers (shuffler + phone), got %d", len(tableCfg.Transformers))
	}
}

// TestBuildRuleListVM vérifie que le VM est correctement construit depuis la config.
func TestBuildRuleListVM(t *testing.T) {
	cfg := &config.Config{
		Tables: []config.TableConfig{
			{Name: "users", Transformers: []config.TransformerConfig{
				{Name: "none", Type: "none", Options: map[string]any{"column_name": "name"}},
				{Name: "shuffler", Type: "shuffler", Options: map[string]any{
					"column_names": []string{"email", "phone"},
				}},
				{Name: "generator", Type: "generator", Options: map[string]any{
					"column_name": "id",
					"format":      `\d+`,
				}},
			}},
		},
	}

	vms := buildRuleListVM(cfg, "users")
	if len(vms) != 3 {
		t.Fatalf("expected 3 VMs, got %d", len(vms))
	}
	if vms[0].Cols != "name" {
		t.Errorf("VM[0].Cols: expected %q, got %q", "name", vms[0].Cols)
	}
	if vms[0].Subtitle != "none" {
		t.Errorf("VM[0].Subtitle: expected %q, got %q", "none", vms[0].Subtitle)
	}
	if vms[1].Cols != "email\nphone" {
		t.Errorf("VM[1].Cols: expected %q, got %q", "email\nphone", vms[1].Cols)
	}
	if vms[1].Subtitle != "shuffler" {
		t.Errorf("VM[1].Subtitle: expected %q, got %q", "shuffler", vms[1].Subtitle)
	}
	if vms[2].Cols != "id" {
		t.Errorf("VM[2].Cols: expected %q, got %q", "id", vms[2].Cols)
	}
}

// TestBuildRuleListVM_UnknownTable vérifie que la fonction renvoie nil pour une table inconnue.
func TestBuildRuleListVM_UnknownTable(t *testing.T) {
	cfg := &config.Config{
		Tables: []config.TableConfig{
			{Name: "users", Transformers: []config.TransformerConfig{}},
		},
	}
	vms := buildRuleListVM(cfg, "nonexistent")
	if vms != nil {
		t.Errorf("expected nil for unknown table, got %v", vms)
	}
}
