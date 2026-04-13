package ui

import (
	"database/sql"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"os"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"github.com/triboulin/demodata/pkg/config"
	"github.com/triboulin/demodata/pkg/export"
	"github.com/triboulin/demodata/pkg/inferer"
	"github.com/triboulin/demodata/pkg/ingest"
	"github.com/triboulin/demodata/pkg/transform"
)

type uiState struct {
	inputFilePath  string
	outputFilePath string
	inputFormat    string
	dataset        ingest.Dataset
	selectedTable  string
	db             *sql.DB
	dbDriver       string
	ruleConfig     *config.Config
	selectedIndex  int
	seed           int64
	sampleRate     float64 // 1–100, défaut 100

	initialColOrder map[string][]string // ordre initial des colonnes par table (immuable après chargement)

	rules                *widget.List
	tableSelect          *widget.Select
	typeSelect           *widget.Select
	formatEntry          *widget.Entry
	statusLabel          *widget.Label
	split                *container.Split
	rightBotContainer    *fyne.Container
	resizeMonitorStarted bool
	stopResize           chan struct{}
	updating             bool
}

// ActionType identifie le type d'une action de mutation de la config.
type ActionType string

const (
	ActionSelectRule            ActionType = "select_rule"
	ActionChangeRuleType        ActionType = "change_rule_type"
	ActionChangeGeneratorRegex  ActionType = "change_generator_regex"
	ActionChangeShufflerColumns ActionType = "change_shuffler_columns"
)

// Action représente une mutation atomique de l'état.
type Action struct {
	Type    ActionType
	Index   int
	Value   string
	Columns []string
}

// RuleListItemVM est le view model d'un item de la liste de règles.
type RuleListItemVM struct {
	Cols     string
	Subtitle string
}

// Start démarre l'interface utilisateur.
func Start(inputFilePath *os.File) error {
	a := app.NewWithID("github.com.triboulin.demodata")
	w := a.NewWindow("Data Mixer Dev Tool")
	w.Resize(fyne.NewSize(1200, 760))
	w.CenterOnScreen()

	state := &uiState{selectedIndex: -1, stopResize: make(chan struct{}), sampleRate: 100}

	initialStatus := widget.NewLabel("Please select a dataset file or connect to a database")
	w.SetContent(initialContent(w, state, initialStatus))
	w.SetOnClosed(func() {
		select {
		case <-state.stopResize:
			// déjà fermé
		default:
			close(state.stopResize)
		}
	})
	if inputFilePath != nil {
		state.inputFormat = strings.TrimPrefix(filepath.Ext(inputFilePath.Name()), ".")
		a.Lifecycle().SetOnStarted(func() {
			initialStatus.SetText("Loading initial file...")
			loadDataset(w, state, inputFilePath, inputFilePath.Name(), initialStatus)
		})
	} else {
		w.SetOnDropped(func(position fyne.Position, uris []fyne.URI) {
			if len(uris) == 0 {
				return
			}

			uri := uris[0]
			if uri.Scheme() != "file" {
				return
			}

			path := uri.Path()
			if path == "" {
				return
			}

			f, err := os.Open(path)
			if err != nil {
				return
			}

			loadDataset(w, state, f, path, widget.NewLabel("Dropped file loading..."))
		})
	}

	w.ShowAndRun()

	return nil
}

func initialContent(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {
	fileEntry := widget.NewEntry()
	fileEntry.SetPlaceHolder("Select input file")

	openBtn := widget.NewButton("Open input file", func() {
		d := dialog.NewFileOpen(func(reader fyne.URIReadCloser, err error) {
			if err != nil || reader == nil {
				return
			}
			path := reader.URI().Path()
			fileEntry.SetText(path)
			loadDataset(w, state, reader, path, status)
		}, w)
		d.Show()
		d.Resize(fyne.NewSize(w.Canvas().Size().Width*0.9, w.Canvas().Size().Height*0.9))
	})

	connectDBBtn := widget.NewButton("Connect to database", func() {
		driverSelect := widget.NewSelect([]string{"sqlite", "mysql", "pgx"}, func(string) {})
		driverSelect.SetSelected("sqlite")
		dsnEntry := widget.NewEntry()
		dsnEntry.SetPlaceHolder(":memory: or /path/db.sqlite or DSN")

		form := container.NewVBox(
			widget.NewLabel("Driver"),
			driverSelect,
			widget.NewLabel("DSN"),
			dsnEntry,
		)

		dlg := dialog.NewCustomConfirm("Database connection", "Connect", "Cancel", form, func(ok bool) {
			if !ok {
				return
			}
			if strings.TrimSpace(dsnEntry.Text) == "" {
				status.SetText("DSN is required")
				return
			}

			status.SetText("Connecting to database...")
			go func() {
				db, err := ingest.OpenDB(driverSelect.Selected, strings.TrimSpace(dsnEntry.Text))
				if err != nil {
					fyne.Do(func() { status.SetText("Connection error: " + err.Error()) })
					return
				}

				tables, err := ingest.ListTables(db, driverSelect.Selected)
				if err != nil {
					_ = db.Close()
					fyne.Do(func() { status.SetText("Table listing error: " + err.Error()) })
					return
				}

				dataset, err := ingest.LoadDB(db, driverSelect.Selected, tables)
				if err != nil {
					_ = db.Close()
					fyne.Do(func() { status.SetText("Load error: " + err.Error()) })
					return
				}

				fyne.Do(func() {
					if state.db != nil {
						_ = state.db.Close()
					}
					state.db = db
					state.dbDriver = driverSelect.Selected
					state.dataset = dataset
					state.inputFormat = "db"
					state.ruleConfig = inferer.InferRuleSet(dataset, nil)
					state.initialColOrder = captureColOrder(state.ruleConfig)
					state.selectedIndex = -1
					state.selectedTable = firstTableName(dataset)
					w.SetContent(mainUI(w, state, status))
					status.SetText(fmt.Sprintf("Connected: %d tables loaded", len(dataset)))
				})
			}()
		}, w)
		dlg.Show()
	})

	return container.NewVBox(
		openBtn,
		connectDBBtn,
		status,
	)
}

func loadDataset(w fyne.Window, state *uiState, reader io.ReadCloser, path string, status *widget.Label) {
	format := filepath.Ext(path)
	if len(format) > 0 {
		format = format[1:]
	}

	fyne.Do(func() {
		state.inputFilePath = path
		status.SetText("Loading file...")
		state.dataset = nil
		state.db = nil
		state.dbDriver = ""
		state.ruleConfig = nil
		state.initialColOrder = nil
		state.selectedIndex = -1
		state.rules = nil
		state.inputFormat = format
	})

	go func() {
		defer reader.Close()
		dataset, colOrder, loadErr := ingest.Load(reader, format)
		fyne.Do(func() {
			if loadErr != nil {
				status.SetText("Load error: " + loadErr.Error())
				return
			}

			state.dataset = dataset
			ext := filepath.Ext(path)
			state.outputFilePath = strings.TrimSuffix(path, ext) + ".out" + ext
			state.ruleConfig = inferer.InferRuleSet(dataset, colOrder)
			state.initialColOrder = captureColOrder(state.ruleConfig)
			state.selectedTable = firstTableName(dataset)
			state.selectedIndex = -1
			state.rules = nil

			w.SetContent(mainUI(w, state, status))
			status.SetText("Loaded " + filepath.Base(path))
		})
	}()
}

func mainUI(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {
	if state.ruleConfig == nil || len(state.ruleConfig.Tables) == 0 {
		status.SetText("No inferred rules available")
	}

	state.typeSelect = widget.NewSelect([]string{"none", "masker", "shuffler", "generator"}, func(string) {})
	state.typeSelect.PlaceHolder = "rule type"
	state.typeSelect.OnChanged = func(newType string) {
		if state.updating || state.selectedIndex < 0 {
			return
		}
		applyAction(w, state, status, Action{Type: ActionChangeRuleType, Index: state.selectedIndex, Value: newType})
	}

	state.formatEntry = widget.NewEntry()
	state.formatEntry.SetPlaceHolder("format (regex)")
	state.formatEntry.OnChanged = func(s string) {
		if state.updating || state.selectedIndex < 0 {
			return
		}
		applyAction(w, state, status, Action{Type: ActionChangeGeneratorRegex, Index: state.selectedIndex, Value: s})
	}

	state.statusLabel = status

	// Compute ideal left-panel width: fit longest list label, capped at 1/3 of window width.
	windowWidth := w.Canvas().Size().Width
	if windowWidth <= 0 {
		windowWidth = 1200
	}
	idealWidth := float32(160) // minimum fallback
	tableCfg := tableConfigForSelected(state)
	if tableCfg != nil {
		for _, t := range tableCfg.Transformers {
			cols, _ := ruleListDisplayParts(t)
			lbl := widget.NewLabel(cols)
			if needed := lbl.MinSize().Width + 48; needed > idealWidth { // 48 = list padding + scrollbar
				idealWidth = needed
			}
		}
	}
	if max := windowWidth / 3; idealWidth > max {
		idealWidth = max
	}
	rightWidth := windowWidth - idealWidth

	left := leftPane(w, state, status)
	rtop := topRight(w, state, status, rightWidth)
	rbot := bottomRight(w, state, status)
	state.rightBotContainer = container.NewStack(rbot)

	right := container.NewBorder(
		container.NewVBox(rtop, widget.NewSeparator()),
		nil, nil, nil,
		state.rightBotContainer,
	)

	split := container.NewHSplit(left, right)
	state.split = split
	if !state.resizeMonitorStarted {
		state.resizeMonitorStarted = true
		go watchWindowResize(w, state, state.stopResize)
	}
	split.SetOffset(idealLeftOffset(windowWidth, idealWidth))
	return split
}

// buildRuleListVM construit un view model pur pour la liste de règles à partir de la config.
func buildRuleListVM(cfg *config.Config, table string) []RuleListItemVM {
	if cfg == nil {
		return nil
	}
	for _, t := range cfg.Tables {
		if t.Name == table {
			vms := make([]RuleListItemVM, len(t.Transformers))
			for i, r := range t.Transformers {
				cols, sub := ruleListDisplayParts(r)
				vms[i] = RuleListItemVM{Cols: cols, Subtitle: sub}
			}
			return vms
		}
	}
	return nil
}

// applyAction mutate state.ruleConfig selon l'action, puis déclenche rebuildUIFromConfig.
func applyAction(w fyne.Window, state *uiState, status *widget.Label, action Action) {
	tableCfg := tableConfigForSelected(state)
	switch action.Type {
	case ActionSelectRule:
		state.selectedIndex = action.Index

	case ActionChangeRuleType:
		if tableCfg == nil || action.Index < 0 || action.Index >= len(tableCfg.Transformers) {
			return
		}
		r := &tableCfg.Transformers[action.Index]
		rt := strings.ToLower(strings.TrimSpace(action.Value))
		r.Type = rt
		r.Name = rt
		if r.Options == nil {
			r.Options = map[string]any{}
		}
		if rt == "shuffler" {
			if len(parseShufflerColumns(r.Options["column_names"])) == 0 {
				if col, ok := r.Options["column_name"].(string); ok && strings.TrimSpace(col) != "" {
					r.Options["column_names"] = []string{strings.TrimSpace(col)}
				}
			}
		} else {
			delete(r.Options, "column_names")
		}
		if rt != "generator" {
			delete(r.Options, "format")
		}

	case ActionChangeGeneratorRegex:
		if tableCfg == nil || action.Index < 0 || action.Index >= len(tableCfg.Transformers) {
			return
		}
		r := &tableCfg.Transformers[action.Index]
		if !strings.EqualFold(r.Type, "generator") {
			return
		}
		if r.Options == nil {
			r.Options = map[string]any{}
		}
		if strings.TrimSpace(action.Value) != "" {
			r.Options["format"] = strings.TrimSpace(action.Value)
		} else {
			delete(r.Options, "format")
		}

	case ActionChangeShufflerColumns:
		if tableCfg == nil || action.Index < 0 || action.Index >= len(tableCfg.Transformers) {
			return
		}
		applyShufflerColumnsAction(state, tableCfg, action.Index, action.Columns)
		// Réordonner selon l'ordre initial des colonnes pour stabiliser la liste.
		if order, ok := state.initialColOrder[state.selectedTable]; ok && len(order) > 0 {
			tableCfg.Transformers, state.selectedIndex = sortTransformersByColOrder(
				tableCfg.Transformers, order, state.selectedIndex)
		}
	}
	rebuildUIFromConfig(w, state, status)
}

// applyShufflerColumnsAction gère les invariants merge/split/dissolve du shuffler.
// Principe de stabilité : les items existants sous le shuffler ne doivent pas décaler.
//   - Merge (≥2 cols) : le shuffler reste à sa place ; les cols décochées sont appendées en fin.
//   - Dissolution (<2 cols) : la 1re col restaurée remplace le shuffler à son slot exact ;
//     les suivantes sont appendées en fin.
func applyShufflerColumnsAction(state *uiState, tableCfg *config.TableConfig, idx int, sel []string) {
	selected := uniqueNonEmpty(sel)
	current := tableCfg.Transformers[idx]
	oldSelected := parseShufflerColumns(current.Options["column_names"])

	selectedSet := make(map[string]struct{}, len(selected))
	for _, c := range selected {
		selectedSet[c] = struct{}{}
	}

	// Colonnes retirées du shuffler (étaient dedans, ne le sont plus).
	removedCols := make([]string, 0)
	for _, c := range oldSelected {
		if _, kept := selectedSet[c]; !kept {
			removedCols = append(removedCols, c)
		}
	}

	newTransformers := make([]config.TransformerConfig, 0, len(tableCfg.Transformers)+len(removedCols))

	if len(selected) >= 2 {
		shufflerRule := current
		shufflerRule.Type = "shuffler"
		shufflerRule.Name = "shuffler"
		if shufflerRule.Options == nil {
			shufflerRule.Options = map[string]any{}
		}
		shufflerRule.Options["column_names"] = selected
		delete(shufflerRule.Options, "column_name")
		delete(shufflerRule.Options, "format")
		delete(shufflerRule.Options, "values")

		newIdx := 0
		for i, t := range tableCfg.Transformers {
			if i == idx {
				newIdx = len(newTransformers)
				newTransformers = append(newTransformers, shufflerRule)
				continue
			}
			colName, _ := t.Options["column_name"].(string)
			if colName != "" {
				if _, grouped := selectedSet[colName]; grouped {
					// Absorber dans le shuffler : supprimer la règle mono-colonne.
					continue
				}
			}
			newTransformers = append(newTransformers, t)
		}
		// Colonnes décochées appendées en fin : les items existants ne décalent pas.
		for _, col := range removedCols {
			newTransformers = append(newTransformers, newSingleColumnRule(col))
		}
		tableCfg.Transformers = newTransformers
		state.selectedIndex = newIdx
		return
	}

	// Dissolution (< 2 colonnes) : la 1re col restaurée remplace le shuffler à sa position
	// exacte en CONSERVANT le type shuffler (solo), les autres deviennent "none" en fin.
	restoreCols := uniqueNonEmpty(oldSelected)
	if len(restoreCols) == 0 {
		restoreCols = uniqueNonEmpty(selected)
	}
	restoredAt := -1
	for i, t := range tableCfg.Transformers {
		if i == idx {
			if len(restoreCols) > 0 {
				restoredAt = len(newTransformers)
				// La colonne principale conserve le type shuffler (solo).
				newTransformers = append(newTransformers, config.TransformerConfig{
					Name: "shuffler",
					Type: "shuffler",
					Options: map[string]any{
						"column_names": []string{restoreCols[0]},
					},
				})
			}
			continue
		}
		newTransformers = append(newTransformers, t)
	}
	// Colonnes restituées appendées en fin comme règles "none".
	if len(restoreCols) > 1 {
		for _, col := range restoreCols[1:] {
			newTransformers = append(newTransformers, newSingleColumnRule(col))
		}
	}
	tableCfg.Transformers = newTransformers
	switch {
	case len(newTransformers) == 0:
		state.selectedIndex = -1
	case restoredAt >= 0:
		state.selectedIndex = restoredAt
	case state.selectedIndex >= len(newTransformers):
		state.selectedIndex = len(newTransformers) - 1
	}
}

// rebuildUIFromConfig rafraîchit la liste et le panneau droit depuis la config courante.
func rebuildUIFromConfig(w fyne.Window, state *uiState, status *widget.Label) {
	state.updating = true
	defer func() { state.updating = false }()

	if state.rules != nil {
		state.rules.Refresh()
		// Re-sélectionner explicitement l'item pour synchroniser le highlight visuel
		// du widget.List avec state.selectedIndex (sans déclencher OnSelected car
		// state.updating == true pendant tout ce bloc).
		if state.selectedIndex >= 0 {
			state.rules.Select(state.selectedIndex)
		}
	}

	if state.selectedIndex >= 0 {
		tableCfg := tableConfigForSelected(state)
		if tableCfg != nil && state.selectedIndex < len(tableCfg.Transformers) {
			r := tableCfg.Transformers[state.selectedIndex]
			if state.typeSelect != nil {
				state.typeSelect.SetSelected(r.Type)
			}
			if state.formatEntry != nil {
				if f, ok := r.Options["format"].(string); ok {
					state.formatEntry.SetText(f)
				} else {
					state.formatEntry.SetText("")
				}
			}
		}
	}

	if state.rightBotContainer != nil {
		state.rightBotContainer.Objects = []fyne.CanvasObject{bottomRight(w, state, status)}
		state.rightBotContainer.Refresh()
	}
}

func leftPane(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {

	tableNames := sortedTableNames(state.dataset)
	state.tableSelect = widget.NewSelect(tableNames, func(selected string) {
		if selected == state.selectedTable {
			return
		}
		state.selectedTable = selected
		state.selectedIndex = -1
		w.SetContent(mainUI(w, state, status))
	})
	if state.selectedTable != "" {
		state.tableSelect.SetSelected(state.selectedTable)
	}
	state.rules = widget.NewList(
		func() int {
			tableCfg := tableConfigForSelected(state)
			if tableCfg == nil {
				return 0
			}
			return len(tableCfg.Transformers)
		},
		func() fyne.CanvasObject {
			return widget.NewRichText(
				&widget.TextSegment{
					Text:  "",
					Style: widget.RichTextStyle{ColorName: theme.ColorNameForeground, Inline: false},
				},
				&widget.TextSegment{
					Text:  "",
					Style: widget.RichTextStyle{ColorName: theme.ColorNameDisabled, SizeName: theme.SizeNameCaptionText, Inline: false},
				},
			)
		},
		func(i widget.ListItemID, o fyne.CanvasObject) {
			tableCfg := tableConfigForSelected(state)
			if tableCfg == nil || i >= len(tableCfg.Transformers) {
				return
			}
			r := tableCfg.Transformers[i]
			cols, subtitle := ruleListDisplayParts(r)
			rt := o.(*widget.RichText)
			rt.Segments[0].(*widget.TextSegment).Text = cols
			rt.Segments[1].(*widget.TextSegment).Text = subtitle
			rt.Refresh()
			state.rules.SetItemHeight(i, rt.MinSize().Height+8)
		},
	)

	state.rules.OnSelected = func(id widget.ListItemID) {
		if state.updating {
			return
		}
		tableCfg := tableConfigForSelected(state)
		if tableCfg == nil || id < 0 || id >= len(tableCfg.Transformers) {
			return
		}
		applyAction(w, state, status, Action{Type: ActionSelectRule, Index: id})
	}

	if len(tableNames) > 1 {
		top := container.NewVBox(
			widget.NewLabel("Table"),
			state.tableSelect,
			widget.NewSeparator(),
		)
		return container.NewBorder(top, nil, nil, nil, state.rules)
	} else {
		return state.rules
	}

}

func topRight(w fyne.Window, state *uiState, status *widget.Label, availWidth float32) fyne.CanvasObject {
	runBtn := widget.NewButton("Execute >", func() {
		if state.ruleConfig == nil || state.dataset == nil {
			status.SetText("Load data first")
			return
		}

		target := transform.ApplyRules(copyDataset(state.dataset), state.ruleConfig, state.seed)
		if state.sampleRate < 100 {
			target = transform.SampleDataset(target, state.sampleRate/100.0, state.seed)
			// Préserver l'intégrité référentielle si la cible est une BDD
			if state.db != nil {
				var tableNames []string
				for t := range target {
					tableNames = append(tableNames, t)
				}
				if fkRels, err := ingest.GetForeignKeys(state.db, state.dbDriver, tableNames); err == nil && len(fkRels) > 0 {
					target = transform.FilterFKViolations(target, fkRels)
				}
			}
		}
		if state.db != nil {
			if err := export.ExportToDB(state.db, target, state.dbDriver); err != nil {
				status.SetText("DB export error: " + err.Error())
				return
			}
			status.SetText("DB export successful")
			return
		}

		outFormat := strings.TrimPrefix(filepath.Ext(state.outputFilePath), ".")
		if outFormat == "" {
			status.SetText("Output extension required")
			return
		}
		if err := export.ExportToFile(target, state.outputFilePath, outFormat, state.initialColOrder); err != nil {
			status.SetText("Export error: " + err.Error())
			return
		}
		status.SetText("Export successful: " + state.outputFilePath)
	})

	var outputInfo fyne.CanvasObject
	if state.db != nil {
		outputInfo = container.NewHBox(widget.NewLabel("Output"), widget.NewLabel("Database (in place)"))
	} else {
		outputInfo = container.NewHBox(widget.NewLabel("Output"), widget.NewLabel(path.Base(state.outputFilePath)))
	}
	controls := container.NewHBox(newSampleControl(state), runBtn)

	l := &wrapBarLayout{shouldWrap: availWidth > 0 && outputInfo.MinSize().Width+controls.MinSize().Width+theme.Padding() > availWidth}
	c := container.New(l, outputInfo, controls)
	l.c = c
	return c
}

// newSampleControl crée un curseur de 1 à 100 % pour l'échantillonnage.
func newSampleControl(state *uiState) fyne.CanvasObject {
	if state.sampleRate <= 0 {
		state.sampleRate = 100
	}
	lbl := widget.NewLabel(fmt.Sprintf("%.0f%%", state.sampleRate))
	slider := widget.NewSlider(1, 100)
	slider.Step = 5
	slider.Value = state.sampleRate
	slider.OnChanged = func(v float64) {
		state.sampleRate = v
		lbl.SetText(fmt.Sprintf("%.0f%%", v))
	}
	sliderWrap := container.New(&minSizeLayout{w: 80}, slider)
	return container.NewHBox(widget.NewLabel("Sample"), sliderWrap, lbl)
}

func bottomRight(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {
	if state.selectedIndex < 0 {
		return container.NewVBox(
			widget.NewLabel("Rule editor"),
			widget.NewLabel("Select a rule on the left"),
		)
	}

	columns := tableColumnsForSelected(state)
	shufflerColumns := widget.NewCheckGroup(columns, nil)

	// Pré-sélectionne les colonnes actuellement configurées.
	tableCfgInit := tableConfigForSelected(state)
	if tableCfgInit != nil && state.selectedIndex < len(tableCfgInit.Transformers) {
		r := tableCfgInit.Transformers[state.selectedIndex]
		if strings.EqualFold(r.Type, "shuffler") {
			if presel := parseShufflerColumns(r.Options["column_names"]); len(presel) > 0 {
				shufflerColumns.SetSelected(presel)
			}
		}
	}

	shufflerColumns.OnChanged = func(sel []string) {
		if state.updating || state.selectedIndex < 0 {
			return
		}
		applyAction(w, state, status, Action{Type: ActionChangeShufflerColumns, Index: state.selectedIndex, Columns: sel})
	}

	return container.NewVBox(
		container.NewPadded(container.NewHBox(
			widget.NewLabel("Rule type"),
			state.typeSelect)),
		ruleEditorFields(state, shufflerColumns),
	)
}

func ruleEditorFields(state *uiState, shufflerColumns *widget.CheckGroup) fyne.CanvasObject {
	selectedType := strings.ToLower(strings.TrimSpace(state.typeSelect.Selected))

	if selectedType == "shuffler" {
		return container.NewVBox(
			shufflerColumns,
		)
	}
	if selectedType == "generator" {
		return container.NewHBox(
			widget.NewLabel("Format / regex"),
			state.formatEntry,
		)
	}

	return container.NewVBox()
}

// wrapBarLayout place ses enfants en ligne unique quand ils tiennent,
// sinon les empile verticalement. Le changement d'état déclenche un Refresh asynchrone
// pour que le conteneur parent recalcule sa hauteur.
type wrapBarLayout struct {
	shouldWrap bool
	c          *fyne.Container
}

func (l *wrapBarLayout) MinSize(objects []fyne.CanvasObject) fyne.Size {
	p := theme.Padding()
	if l.shouldWrap {
		var maxW, totalH float32
		for i, o := range objects {
			ms := o.MinSize()
			if ms.Width > maxW {
				maxW = ms.Width
			}
			if i > 0 {
				totalH += p
			}
			totalH += ms.Height
		}
		return fyne.NewSize(maxW, totalH)
	}
	var totalW, maxH float32
	for i, o := range objects {
		ms := o.MinSize()
		if i > 0 {
			totalW += p
		}
		totalW += ms.Width
		if ms.Height > maxH {
			maxH = ms.Height
		}
	}
	return fyne.NewSize(totalW, maxH)
}

func (l *wrapBarLayout) Layout(objects []fyne.CanvasObject, size fyne.Size) {
	p := theme.Padding()
	needed := float32(0)
	for i, o := range objects {
		if i > 0 {
			needed += p
		}
		needed += o.MinSize().Width
	}
	nowWrap := needed > size.Width
	if nowWrap != l.shouldWrap {
		l.shouldWrap = nowWrap
		if l.c != nil {
			c := l.c
			go func() { fyne.Do(func() { c.Refresh() }) }()
		}
	}
	if !l.shouldWrap {
		x := float32(0)
		for _, o := range objects {
			ms := o.MinSize()
			o.Move(fyne.NewPos(x, (size.Height-ms.Height)/2))
			o.Resize(ms)
			x += ms.Width + p
		}
	} else {
		y := float32(0)
		for _, o := range objects {
			ms := o.MinSize()
			o.Move(fyne.NewPos(0, y))
			o.Resize(fyne.NewSize(size.Width, ms.Height))
			y += ms.Height + p
		}
	}
}

func idealLeftOffset(windowWidth, idealWidth float32) float64 {
	if windowWidth <= 0 {
		return 1.0 / 3.0
	}
	return float64(idealWidth / windowWidth)
}

// minSizeLayout est un fyne.Layout minimal qui impose une largeur minimale.
type minSizeLayout struct{ w float32 }

func (m *minSizeLayout) Layout(objs []fyne.CanvasObject, size fyne.Size) {
	for _, o := range objs {
		o.Resize(size)
		o.Move(fyne.NewPos(0, 0))
	}
}
func (m *minSizeLayout) MinSize(_ []fyne.CanvasObject) fyne.Size {
	return fyne.NewSize(m.w, 0)
}

func watchWindowResize(w fyne.Window, state *uiState, stop <-chan struct{}) {
	lastW := w.Canvas().Size().Width
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			size := w.Canvas().Size()
			if size.Width == lastW {
				continue
			}
			lastW = size.Width
			if state == nil || state.split == nil {
				continue
			}
			windowWidth := size.Width
			idealWidth := float32(160)
			tableCfg := tableConfigForSelected(state)
			if tableCfg != nil {
				for _, t := range tableCfg.Transformers {
					cols, _ := ruleListDisplayParts(t)
					lbl := widget.NewLabel(cols)
					if needed := lbl.MinSize().Width + 48; needed > idealWidth {
						idealWidth = needed
					}
				}
			}
			if max := windowWidth / 3; idealWidth > max {
				idealWidth = max
			}
			offset := float64(idealWidth / windowWidth)
			fyne.Do(func() {
				if state.split != nil {
					state.split.SetOffset(offset)
				}
			})
		}
	}
}

func sortedTableNames(dataset ingest.Dataset) []string {
	names := make([]string, 0, len(dataset))
	for name := range dataset {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func firstTableName(dataset ingest.Dataset) string {
	names := sortedTableNames(dataset)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func tableConfigForSelected(state *uiState) *config.TableConfig {
	if state == nil || state.ruleConfig == nil {
		return nil
	}
	for i := range state.ruleConfig.Tables {
		if state.ruleConfig.Tables[i].Name == state.selectedTable {
			return &state.ruleConfig.Tables[i]
		}
	}
	return nil
}

func copyDataset(in ingest.Dataset) ingest.Dataset {
	out := make(ingest.Dataset, len(in))
	for table, records := range in {
		copied := make([]ingest.Record, len(records))
		for i := range records {
			clone := make(ingest.Record, len(records[i]))
			for k, v := range records[i] {
				clone[k] = v
			}
			copied[i] = clone
		}
		out[table] = copied
	}
	return out
}

func tableColumnsForSelected(state *uiState) []string {
	table := state.selectedTable
	if table == "" {
		table = firstTableName(state.dataset)
	}
	records, ok := state.dataset[table]
	if !ok || len(records) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	columns := make([]string, 0, len(records[0]))
	for _, rec := range records {
		for k := range rec {
			if _, exists := seen[k]; exists {
				continue
			}
			seen[k] = struct{}{}
			columns = append(columns, k)
		}
	}
	sort.Strings(columns)
	return columns
}

func parseShufflerColumns(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return uniqueNonEmpty(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return uniqueNonEmpty(out)
	case string:
		parts := strings.Split(v, ",")
		return uniqueNonEmpty(parts)
	default:
		return nil
	}
}

func uniqueNonEmpty(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
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

// captureColOrder enregistre l'ordre initial des colonnes par table depuis la config inférée.
// Cette snapshot est immuable : elle sert de référence pour re-trier la liste après chaque mutation.
func captureColOrder(cfg *config.Config) map[string][]string {
	if cfg == nil {
		return nil
	}
	order := make(map[string][]string, len(cfg.Tables))
	for _, t := range cfg.Tables {
		cols := make([]string, 0, len(t.Transformers))
		for _, r := range t.Transformers {
			for _, c := range transformerPrimaryColumns(r) {
				if c != "" {
					cols = append(cols, c)
				}
			}
		}
		order[t.Name] = cols
	}
	return order
}

// transformerPrimaryColumns retourne les colonnes associées à une règle (utile pour le tri).
func transformerPrimaryColumns(r config.TransformerConfig) []string {
	if strings.EqualFold(r.Type, "shuffler") {
		return parseShufflerColumns(r.Options["column_names"])
	}
	if col, _ := r.Options["column_name"].(string); col != "" {
		return []string{col}
	}
	return nil
}

// sortTransformersByColOrder retrie les transformers selon l'ordre initial des colonnes.
// Retourne la slice triée et le nouveau selectedIndex (suivi de l'item sélectionné).
func sortTransformersByColOrder(transformers []config.TransformerConfig, colOrder []string, selectedIndex int) ([]config.TransformerConfig, int) {
	if len(colOrder) == 0 || len(transformers) <= 1 {
		return transformers, selectedIndex
	}
	// Rang de chaque colonne dans l'ordre initial.
	rank := make(map[string]int, len(colOrder))
	for i, c := range colOrder {
		rank[c] = i
	}
	rankOf := func(r config.TransformerConfig) int {
		min := len(colOrder) // valeur par défaut : en fin de liste
		for _, c := range transformerPrimaryColumns(r) {
			if i, ok := rank[c]; ok && i < min {
				min = i
			}
		}
		return min
	}
	type indexed struct {
		t    config.TransformerConfig
		orig int
	}
	items := make([]indexed, len(transformers))
	for i, t := range transformers {
		items[i] = indexed{t, i}
	}
	sort.SliceStable(items, func(i, j int) bool {
		return rankOf(items[i].t) < rankOf(items[j].t)
	})
	sorted := make([]config.TransformerConfig, len(items))
	newSelected := selectedIndex
	for newIdx, item := range items {
		sorted[newIdx] = item.t
		if item.orig == selectedIndex {
			newSelected = newIdx
		}
	}
	return sorted, newSelected
}

func newSingleColumnRule(column string) config.TransformerConfig {
	col := strings.TrimSpace(column)
	return config.TransformerConfig{
		Name: "none",
		Type: "none",
		Options: map[string]any{
			"column_name": col,
		},
	}
}

// ruleListDisplayParts retourne la ligne principale (colonne(s)) et le sous-titre (type)
// à afficher dans la liste de gauche pour une règle donnée.
func ruleListDisplayParts(r config.TransformerConfig) (cols, subtitle string) {
	switch strings.ToLower(r.Type) {
	case "shuffler":
		colNames := parseShufflerColumns(r.Options["column_names"])
		return strings.Join(colNames, "\n"), r.Type
	default:
		col, _ := r.Options["column_name"].(string)
		return col, r.Type
	}
}
