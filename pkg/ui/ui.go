package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"

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
	filePath      string
	records       []ingest.Record
	ruleConfig    *config.Config
	selectedIndex int

	list        *widget.List
	columnEntry *widget.Entry
	typeSelect  *widget.Select
	formatEntry *widget.Entry
	valuesEntry *widget.Entry
	statusLabel *widget.Label
}

// Start démarre l'interface utilisateur.
func Start() error {
	a := app.New()
	w := a.NewWindow("Data Mixer Dev Tool")
	w.Resize(fyne.NewSize(600, 300))

	state := &uiState{selectedIndex: -1}

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
		defer f.Close()

		loadDataset(w, state, f, path, widget.NewLabel("Dropped file loading..."))
	})

	w.SetContent(initialContent(w, state))
	w.ShowAndRun()

	return nil
}

func initialContent(w fyne.Window, state *uiState) fyne.CanvasObject {
	fileEntry := widget.NewEntry()
	fileEntry.SetPlaceHolder("Select input file")

	status := widget.NewLabel("Please select or drag & drop a dataset file (CSV/JSON/XLSX)")
	openBtn := widget.NewButton("Open input file", func() {
		dialog.ShowFileOpen(func(reader fyne.URIReadCloser, err error) {
			if err != nil || reader == nil {
				return
			}
			defer reader.Close()
			path := reader.URI().Path()
			fileEntry.SetText(path)
			loadDataset(w, state, reader, path, status)
		}, w)
	})

	return container.NewVBox(
		openBtn,
		status,
	)
}

func loadDataset(w fyne.Window, state *uiState, reader io.Reader, path string, status *widget.Label) {
	state.filePath = path
	status.SetText("Loading file...")

	format := filepath.Ext(path)
	if len(format) > 0 {
		format = format[1:]
	}

	recs, loadErr := ingest.Load(reader, format)
	if loadErr != nil {
		status.SetText("Load error: " + loadErr.Error())
		return
	}

	state.records = recs
	state.ruleConfig = inferer.InferRuleSet(recs)
	state.selectedIndex = -1

	state.list = nil

	w.Resize(fyne.NewSize(1250, 760))
	w.SetContent(mainUI(w, state, status))
}

func mainUI(w fyne.Window, state *uiState, status *widget.Label) fyne.CanvasObject {
	if state.ruleConfig == nil || len(state.ruleConfig.Tables) == 0 {
		status.SetText("No inferred rules available")
	}
	w.Resize(fyne.NewSize(1200, 760))
	state.list = widget.NewList(
		func() int {
			if state.ruleConfig == nil || len(state.ruleConfig.Tables) == 0 {
				return 0
			}
			return len(state.ruleConfig.Tables[0].Transformers)
		},
		func() fyne.CanvasObject { return widget.NewLabel("rule") },
		func(i widget.ListItemID, o fyne.CanvasObject) {
			if state.ruleConfig == nil || len(state.ruleConfig.Tables) == 0 {
				return
			}
			r := state.ruleConfig.Tables[0].Transformers[i]
			o.(*widget.Label).SetText(fmt.Sprintf("%d. %s (%s)", i+1, r.Name, r.Type))
		},
	)

	state.list.OnSelected = func(id widget.ListItemID) {
		state.selectedIndex = id
		r := state.ruleConfig.Tables[0].Transformers[id]
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

	state.columnEntry = widget.NewEntry()
	state.columnEntry.SetPlaceHolder("column_name")

	state.typeSelect = widget.NewSelect([]string{"sampler", "masker", "shuffler", "generator"}, func(string) {})
	state.typeSelect.PlaceHolder = "rule type"

	state.formatEntry = widget.NewEntry()
	state.formatEntry.SetPlaceHolder("format (regex)")

	state.valuesEntry = widget.NewMultiLineEntry()
	state.valuesEntry.SetPlaceHolder("values as JSON array or empty")

	saveRuleBtn := widget.NewButton("Save rule", func() {
		if state.selectedIndex < 0 || state.ruleConfig == nil || len(state.ruleConfig.Tables) == 0 {
			status.SetText("Select a rule first")
			return
		}

		r := &state.ruleConfig.Tables[0].Transformers[state.selectedIndex]
		r.Type = state.typeSelect.Selected
		r.Name = fmt.Sprintf("%s_%s", r.Type, state.columnEntry.Text)
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
		state.list.Refresh()
	})

	saveConfigBtn := widget.NewButton("Save ruleset config", func() {
		dialog.ShowFileSave(func(uri fyne.URIWriteCloser, err error) {
			if err != nil || uri == nil {
				return
			}
			defer uri.Close()
			b, err := json.MarshalIndent(state.ruleConfig, "", "  ")
			if err != nil {
				status.SetText("Error serializing config: " + err.Error())
				return
			}
			if err := os.WriteFile(uri.URI().Path(), b, 0644); err != nil {
				status.SetText("Error writing config: " + err.Error())
				return
			}
			status.SetText("Ruleset config saved: " + uri.URI().Path())
		}, w)
	})

	outputEntry := widget.NewEntry()
	outputEntry.SetPlaceHolder("output file path")

	chooseOutputBtn := widget.NewButton("Choose output file", func() {
		dialog.ShowFileSave(func(uri fyne.URIWriteCloser, err error) {
			if err != nil || uri == nil {
				return
			}
			outputEntry.SetText(uri.URI().Path())
			uri.Close()
		}, w)
	})

	applyBtn := widget.NewButton("Apply rules and export", func() {
		if state.ruleConfig == nil || state.records == nil {
			status.SetText("Load data first")
			return
		}
		if outputEntry.Text == "" {
			status.SetText("Choose output file first")
			return
		}

		target := transform.ApplyRules(append([]ingest.Record(nil), state.records...), state.ruleConfig, 42)
		outFormat := strings.TrimPrefix(filepath.Ext(outputEntry.Text), ".")
		if outFormat == "" {
			status.SetText("Output extension required")
			return
		}
		if err := export.ExportToFile(target, outputEntry.Text, outFormat); err != nil {
			status.SetText("Export error: " + err.Error())
			return
		}
		status.SetText("Export successful: " + outputEntry.Text)
	})

	state.statusLabel = status

	left := container.NewVBox(
		widget.NewLabel("Rules"),
		state.list,
		saveConfigBtn,
		widget.NewButton("Add rule", func() {
			if state.ruleConfig == nil {
				state.ruleConfig = &config.Config{Tables: []config.TableConfig{{Name: "default", Transformers: []config.TransformerConfig{}}}}
			}

			if len(state.ruleConfig.Tables) == 0 {
				state.ruleConfig.Tables = append(state.ruleConfig.Tables, config.TableConfig{Name: "default", Transformers: []config.TransformerConfig{}})
			}

			newRule := config.TransformerConfig{
				Name:    "new_rule",
				Type:    "sampler",
				Options: map[string]any{"column_name": "", "format": "", "values": []any{}},
			}
			state.ruleConfig.Tables[0].Transformers = append(state.ruleConfig.Tables[0].Transformers, newRule)
			state.selectedIndex = len(state.ruleConfig.Tables[0].Transformers) - 1
			state.list.Refresh()

			// Rebuild main UI to reflect state changes
			w.SetContent(mainUI(w, state, status))
		}),
	)

	var r1 *fyne.Container
	if state.selectedIndex < 0 {
		r1 = container.NewVBox(
			widget.NewLabel("Rule editor"),
			widget.NewLabel("Select a rule on the left"),
		)
	} else {
		r1 = container.NewVBox(
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
	r2 := container.NewVBox(
		widget.NewLabel("Output"),
		outputEntry,
		chooseOutputBtn,
		applyBtn,
		status,
	)
	right := container.NewVSplit(r1, r2)

	split := container.NewHSplit(left, right)
	split.SetOffset(0.25)
	return split
}
