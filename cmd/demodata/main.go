package main

import (
	"flag"
	"fmt"
	"os"

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
	help := flag.Bool("help", false, "Afficher l'aide")
	useUI := flag.Bool("ui", false, "Lancer l'interface graphique")
	flag.Parse()

	if *help {
		flag.Usage()
		return
	} else if *useUI {
		if err := ui.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "erreur ui : %v\n", err)
			os.Exit(1)
		}
		return
	} else if *inputFile == "" {
		fmt.Fprintln(os.Stderr, "erreur : le fichier d'entrée est requis")
		flag.Usage()
		os.Exit(1)
	} else {
		// READ FILE
		records, err := ingest.LoadFile(*inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "erreur de chargement : %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Données chargées : %d enregistrements\n", len(records))
		if len(records) == 0 {
			fmt.Println("Aucun enregistrement trouvé.")
		}

		if *configFile != "" {
			// Charger les règles de transformation depuis le fichier de config
			config, err := config.LoadFromFile(*configFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "erreur de chargement de config : %v\n", err)
				os.Exit(1)
			}
			if *outputFile == "" {
				fmt.Println("Extrapolation d'une proposition de configuration à partir des données...")
			} else {
				fmt.Printf("Configuration chargée : %d tables configurées\n", len(config.Tables))
				// Appliquer les règles de transformation aux données
				records = transform.ApplyRules(records, config, *seed)
				outputFormat := ingest.GetFileExtension(*outputFile)
				if err := export.ExportToFile(records, *outputFile, outputFormat); err != nil {
					fmt.Fprintf(os.Stderr, "erreur d'export : %v\n", err)
					os.Exit(1)
				}
			}
		}
	}
}
