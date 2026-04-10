# Instructions pour GitHub Copilot / Agents IA

Ce fichier contient des directives pour développer **l'utilitaire Go de génération de données de développement**, avec un moteur de transformation de données et une interface graphique native.

## 🎯 Objectif du projet

Créer un utilitaire Go qui :
- ingère des données tabulaires ou des objets (CSV, JSON, YAML, BDD SQL, XLSX)
- liste les valeurs uniques par colonne et l'unicité des données
- identifie des regex qui régissent les données (ex : faker)
- propose des règles de transformation applicables sur tout ou partie des données (shuffle, mask ou faker)
- mélange / anonymise / transforme ces données
- génère une version safe et réutilisable pour les environnements de développement
- fonctionne en **CLI** et dispose d’une **interface graphique native** (fenêtre desktop)

## 🧩 Architecture suggérée

### 📦 Modules principaux

- `cmd/` : commandes CLI (mode `cli` + option `--ui` pour lancer l’interface)
- `pkg/ingest` : lecture & normalisation des données
- `pkg/schema` : modèle de schéma & validation (PK/FK, contraintes)
- `pkg/transform` : moteur de règles (shuffle, mask, faker)
- `pkg/export` : export vers CSV/JSON/SQL/XLSX ou écriture directe dans une BDD
- `pkg/ui` : interface graphique native ([Fyne](https://fyne.io/))
- `pkg/config` : lecture de configuration YAML/JSON pour règles

### 🧠 Séparation des responsabilités

- Le **cœur métier** (config/ingest/schema/transform/export) doit être indépendant de l'I/O et réutilisable dans :
  - un CLI
  - une interface graphique native
- L’UI doit se contenter d’appeler les fonctions du moteur et d’afficher les résultats.

## 🛠️ Exigences clés

- Seed aléatoire configurable (reproductibilité des jeux de données générés)
- Test unitaire couvrant chaque package
- Performance : traitement en streaming pour les gros fichiers (ne pas tout charger en mémoire)

## ✅ Interface native (non web)

💡 Utiliser une bibliothèque Go pour UI native :
- **Fyne** (cross-platform, recommandé)

L’interface doit proposer :
- import de fichier (drag & drop ou sélection)
- aperçu des données
- visualisation des règles extrapolées et possibilité de les ajuster
- génération et enregistrement du dataset

## 🧭 Conventions & bonnes pratiques

- Packages en minuscules (ingest, transform, export, ui)
- Chaque package doit avoir des tests `*_test.go`
- Documenter les fonctions exportées et structures principales
- Logging léger pour le mode CLI (niveau de log configurable)

## 📌 Notes additionnelles

- La configuration des règles doit rester lisible (YAML/JSON simple)
- L’outil doit rester utilisable en ligne de commande pour les pipelines CI
