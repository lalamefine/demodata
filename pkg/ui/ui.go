package ui

import (
	"database/sql"
	"encoding/json"
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
	selectedRule   *config.TransformerConfig

	rules                *widget.List
	tableSelect          *widget.Select
	columnEntry          *widget.Entry
	typeSelect           *widget.Select
	formatEntry          *widget.Entry
	valuesEntry          *widget.Entry
	statusLabel          *widget.Label
	split                *container.Split
	rightBotContainer    *fyne.Container
	resizeMonitorStarted bool
}

// Start démarre l'interface utilisateur.
func Start(inputFilePath *os.File) error {
	a := app.NewWithID("github.com.triboulin.demodata")
	w := a.NewWindow("Data Mixer Dev Tool")
	w.Resize(fyne.NewSize(600, 300))

	state := &uiState{selectedIndex: -1}

	initialStatus := widget.NewLabel("Please select a dataset file or connect to a database")
	w.SetContent(initialContent(w, state, initialStatus))
	if inputFilePath != nil {
		explodedInputFilePath := strings.Split(inputFilePath.Name(), ".")
		defaultOutputPath := strings.Join(explodedInputFilePath[:len(explodedInputFilePath)-1], ".") + "_out." + explodedInputFilePath[len(explodedInputFilePath)-1]
		state.outputFilePath = defaultOutputPath
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
		dialog.ShowFileOpen(func(reader fyne.URIReadCloser, err error) {
			if err != nil || reader == nil {
				return
			}
			path := reader.URI().Path()
			fileEntry.SetText(path)
			loadDataset(w, state, reader, path, status)
		}, w)
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
					state.ruleConfig = inferer.InferRuleSet(dataset)
					state.selectedIndex = -1
					state.selectedTable = firstTableName(dataset)
					w.Resize(fyne.NewSize(1250, 760))
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
		state.selectedIndex = -1
		state.rules = nil
		state.inputFormat = format
	})

	go func() {
		defer reader.Close()
		dataset, loadErr := ingest.Load(reader, format)
		fyne.Do(func() {
			if loadErr != nil {
				status.SetText("Load error: " + loadErr.Error())
				return
			}

			state.dataset = dataset
			state.ruleConfig = inferer.InferRuleSet(dataset)
			state.selectedTable = firstTableName(dataset)
			state.selectedIndex = -1
			state.rules = nil

			w.Resize(fyne.NewSize(1250, 760))
			w.SetContent(mainUI(w, state, status))
			status.SetText("Loaded " + filepath.Base(path))
		})
	}()
}

func mainUI(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {
	if state.ruleConfig == nil || len(state.ruleConfig.Tables) == 0 {
		status.SetText("No inferred rules available")
	}
	w.Resize(fyne.NewSize(1200, 760))

	state.columnEntry = widget.NewEntry()
	state.columnEntry.SetPlaceHolder("column_name")

	state.typeSelect = widget.NewSelect([]string{"none", "masker", "shuffler", "generator"}, func(string) {})
	state.typeSelect.PlaceHolder = "rule type"

	state.formatEntry = widget.NewEntry()
	state.formatEntry.SetPlaceHolder("format (regex)")

	state.valuesEntry = widget.NewMultiLineEntry()
	state.valuesEntry.SetPlaceHolder("values as JSON array or empty")

	state.statusLabel = status

	left := leftPane(w, state, status)
	rtop := topRight(w, state, status)
	rbot := bottomRight(w, state, status)
	state.rightBotContainer = container.NewStack(rbot)

	// Rafraîchit uniquement le panneau bas-droit lors de la sélection d'une règle.
	prevOnSelected := state.rules.OnSelected
	state.rules.OnSelected = func(id widget.ListItemID) {
		prevOnSelected(id)
		state.rightBotContainer.Objects = []fyne.CanvasObject{bottomRight(w, state, status)}
		state.rightBotContainer.Refresh()
	}

	right := container.NewBorder(
		container.NewVBox(rtop, widget.NewSeparator()),
		nil, nil, nil,
		state.rightBotContainer,
	)

	// Compute ideal left-panel width: fit longest list label, capped at 50% of window width.
	windowWidth := w.Canvas().Size().Width
	if windowWidth <= 0 {
		windowWidth = 1200
	}
	idealWidth := float32(160) // minimum fallback
	tableCfg := tableConfigForSelected(state)
	if tableCfg != nil {
		for i, t := range tableCfg.Transformers {
			lbl := widget.NewLabel(fmt.Sprintf("%d. %s (%s)", i+1, t.Name, t.Type))
			if needed := lbl.MinSize().Width + 48; needed > idealWidth { // 48 = list padding + scrollbar
				idealWidth = needed
			}
		}
	}
	if max := windowWidth / 3; idealWidth > max {
		idealWidth = max
	}

	split := container.NewHSplit(left, right)
	state.split = split
	if !state.resizeMonitorStarted {
		state.resizeMonitorStarted = true
		go watchWindowResize(w, state)
	}
	split.SetOffset(idealLeftOffset(windowWidth, idealWidth))
	return split
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
		func() fyne.CanvasObject { return widget.NewLabel("column") },
		func(i widget.ListItemID, o fyne.CanvasObject) {
			tableCfg := tableConfigForSelected(state)
			if tableCfg == nil {
				return
			}
			r := tableCfg.Transformers[i]
			o.(*widget.Label).SetText(r.Name)
		},
	)

	state.rules.OnSelected = func(id widget.ListItemID) {
		tableCfg := tableConfigForSelected(state)
		if tableCfg == nil || id < 0 || id >= len(tableCfg.Transformers) {
			return
		}
		state.selectedIndex = id
		r := tableCfg.Transformers[id]
		state.selectedRule = &r
		state.columnEntry.SetText(fmt.Sprint(r.Options["column_name"]))
		state.typeSelect.SetSelected(r.Type)
		state.formatEntry.SetText(fmt.Sprint(r.Options["format"]))
		if vals, ok := r.Options["values"]; ok {
			j, _ := json.Marshal(vals)
			state.valuesEntry.SetText(string(j))
		} else {
			state.valuesEntry.SetText("")
		}
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

func topRight(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {
	displayedOutputFileName := path.Base(state.outputFilePath)
	if displayedOutputFileName == "." || displayedOutputFileName == "" {
		displayedOutputFileName = "Select output file"
	}
	var chooseOutputBtn *widget.Button
	chooseOutputBtn = widget.NewButton(displayedOutputFileName, func() {
		dialog.ShowFileSave(func(uri fyne.URIWriteCloser, err error) {
			if err != nil || uri == nil {
				return
			}
			state.outputFilePath = uri.URI().Path()
			uri.Close()
			displayedOutputFileNameSl := strings.Split(state.outputFilePath, string(os.PathSeparator))
			displayedOutputFileName := displayedOutputFileNameSl[len(displayedOutputFileNameSl)-1]
			chooseOutputBtn.SetText(displayedOutputFileName)
		}, w)
	})

	runBtn := widget.NewButton(">", func() {
		if state.ruleConfig == nil || state.dataset == nil {
			status.SetText("Load data first")
			return
		}

		target := transform.ApplyRules(copyDataset(state.dataset), state.ruleConfig, 42)
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
		if state.inputFormat != "" && outFormat != state.inputFormat {
			status.SetText("Output format must match input format")
			return
		}
		if err := export.ExportToFile(target, state.outputFilePath, outFormat); err != nil {
			status.SetText("Export error: " + err.Error())
			return
		}
		status.SetText("Export successful: " + state.outputFilePath)
	})

	if state.db != nil {
		return container.NewHBox(
			widget.NewLabel("Output"),
			widget.NewLabel("Database (in place)"),
			runBtn,
		)
	}

	return container.NewHBox(
		widget.NewLabel("Output"),
		chooseOutputBtn,
		runBtn,
	)
}

func bottomRight(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {
	saveRuleBtn := widget.NewButton("Save rule", func() {
		tableCfg := tableConfigForSelected(state)
		if state.selectedIndex < 0 || tableCfg == nil {
			status.SetText("Select a rule first")
			return
		}

		r := &tableCfg.Transformers[state.selectedIndex]
		r.Type = state.typeSelect.Selected
		r.Name = fmt.Sprintf("%s - %s", state.columnEntry.Text, r.Type)
		if r.Options == nil {
			r.Options = map[string]any{}
		}
		r.Options["column_name"] = state.columnEntry.Text
		r.Options["format"] = state.formatEntry.Text
		if state.valuesEntry.Text != "" {
			var arr []any
			if err := json.Unmarshal([]byte(state.valuesEntry.Text), &arr); err == nil {
				r.Options["values"] = arr
			} else {
				status.SetText("Values must be JSON array")
				return
			}
		}

		status.SetText("Rule saved")
		state.rules.Refresh()

		// Rebuild to recompute left panel width from current rule labels.
		w.SetContent(mainUI(w, state, status))
	})

	if state.selectedIndex < 0 {
		return container.NewVBox(
			widget.NewLabel("Rule editor"),
			widget.NewLabel("Select a rule on the left"),
		)
	}

	return container.NewVBox(
		widget.NewLabel("Rule editor"),
		widget.NewLabel("Column name"),
		state.columnEntry,
		widget.NewLabel("Rule type"),
		state.typeSelect,
		widget.NewLabel("Format / regex"),
		state.formatEntry,
		widget.NewLabel("Values (JSON array)"),
		state.valuesEntry,
		saveRuleBtn,
	)
}

func idealLeftOffset(windowWidth, idealWidth float32) float64 {
	if windowWidth <= 0 {
		return 1.0 / 3.0
	}
	return float64(idealWidth / windowWidth)
}

func watchWindowResize(w fyne.Window, state *uiState) {
	lastW := w.Canvas().Size().Width
	for {
		time.Sleep(150 * time.Millisecond)
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
			for i, t := range tableCfg.Transformers {
				lbl := widget.NewLabel(fmt.Sprintf("%d. %s (%s)", i+1, t.Name, t.Type))
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
