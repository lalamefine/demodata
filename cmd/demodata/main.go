package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/triboulin/demodata/pkg/config"
	"github.com/triboulin/demodata/pkg/export"
	"github.com/triboulin/demodata/pkg/ingest"
	"github.com/triboulin/demodata/pkg/transform"
	"github.com/triboulin/demodata/pkg/ui"
)

func main() {
	// Définition des flags
	inputFile := flag.String("input", "", "Chemin du fichier source (CSV/JSON/XLSX)")
	outputFile := flag.String("output", "", "Fichier de sortie (CSV/JSON/XLSX)")
	configFile := flag.String("config", "", "Fichier YAML/JSON de règles de transformation")
	seed := flag.Int64("seed", 0, "Seed pseudo-aléatoire pour reproductibilité")
	driver := flag.String("driver", "", "Driver BDD (sqlite|mysql|pgx|pgsql)")
	dsn := flag.String("dsn", "", "Chaîne de connexion BDD")
	help := flag.Bool("help", false, "Afficher l'aide")
	useUI := flag.Bool("ui", false, "Lancer l'interface graphique")
	flag.Parse()

	if *help {
		flag.Usage()
		return
	} else if *useUI {
		var startOnFile *os.File = nil
		if *inputFile != "" {
			startOnFile, _ = os.Open(*inputFile)
		}
		if err := ui.Start(startOnFile); err != nil {
			fmt.Fprintf(os.Stderr, "erreur ui : %v\n", err)
			os.Exit(1)
		}
		return
	} else if *inputFile == "" && *dsn == "" {
		fmt.Fprintln(os.Stderr, "erreur : un input fichier ou une connexion BDD est requis")
		flag.Usage()
		os.Exit(1)
	} else {
		var dataset ingest.Dataset
		var err error
		var dbDriver string
		var dbHandle *sql.DB

		if *dsn != "" {
			dbDriver = *driver
			if dbDriver == "" {
				dbDriver = "sqlite"
			}
			db, openErr := ingest.OpenDB(dbDriver, *dsn)
			if openErr != nil {
				fmt.Fprintf(os.Stderr, "erreur connexion bdd : %v\n", openErr)
				os.Exit(1)
			}
			dbHandle = db
			defer db.Close()

			tables, listErr := ingest.ListTables(db, dbDriver)
			if listErr != nil {
				fmt.Fprintf(os.Stderr, "erreur listing tables : %v\n", listErr)
				os.Exit(1)
			}
			dataset, err = ingest.LoadDB(db, dbDriver, tables)
		} else {
			dataset, err = ingest.LoadFile(*inputFile)
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "erreur de chargement : %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Données chargées : %d enregistrements (%d tables)\n", countRecords(dataset), len(dataset))
		if countRecords(dataset) == 0 {
			fmt.Println("Aucun enregistrement trouvé.")
		}

		if *configFile != "" {
			// Charger les règles de transformation depuis le fichier de config
			config, err := config.LoadFromFile(*configFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "erreur de chargement de config : %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Configuration chargée : %d tables configurées\n", len(config.Tables))
			dataset = transform.ApplyRules(dataset, config, *seed)

			if *dsn != "" {
				if dbHandle == nil {
					fmt.Fprintln(os.Stderr, "erreur : connexion bdd indisponible")
					os.Exit(1)
				}
				if err := export.ExportToDB(dbHandle, dataset, dbDriver); err != nil {
					fmt.Fprintf(os.Stderr, "erreur d'export bdd : %v\n", err)
					os.Exit(1)
				}
				fmt.Println("Export BDD terminé")
				return
			}

			if *outputFile == "" {
				*outputFile = defaultOutputPath(*inputFile)
			}
			inputFormat := ingest.GetFileExtension(*inputFile)
			outputFormat := ingest.GetFileExtension(*outputFile)
			if inputFormat != "" && outputFormat != "" && inputFormat != outputFormat {
				fmt.Fprintf(os.Stderr, "erreur : le format de sortie (%s) doit correspondre a l'entree (%s)\n", outputFormat, inputFormat)
				os.Exit(1)
			}
			if err := export.ExportToFile(dataset, *outputFile, outputFormat); err != nil {
				fmt.Fprintf(os.Stderr, "erreur d'export : %v\n", err)
				os.Exit(1)
			}
		}
	}
}

func countRecords(dataset ingest.Dataset) int {
	total := 0
	for _, records := range dataset {
		total += len(records)
	}
	return total
}

func defaultOutputPath(input string) string {
	ext := filepath.Ext(input)
	base := input[:len(input)-len(ext)]
	if ext == "" {
		return input + "_out"
	}
	return base + "_out" + ext
}
