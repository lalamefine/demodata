# demodata

Utilitaire Go pour ingérer, analyser et transformer des données tabulaires en vue d'un usage dans les environnements de développement.

Fonctionnalités principales :
- Lecture de fichiers **CSV, JSON, XLSX** et de bases de données **SQLite, MySQL, PostgreSQL**
- Analyse automatique des données (types, patterns, sémantique)
- Application de règles de transformation : masquage, mélange, génération
- Échantillonnage avec préservation de l'intégrité référentielle
- Export vers fichier ou réécriture directe en base
- **Interface graphique native** (Fyne) et **mode CLI**

---

## Installation

### Depuis les releases GitHub

Des binaires pré-compilés sont disponibles sur la [page Releases](../../releases) pour Linux et Windows :

| Plateforme | Fichier |
|-----------|---------|
| Linux x86-64 | `demodata-linux-amd64` |
| Windows x86-64 | `demodata-windows-amd64.exe` |

```bash
# Linux
chmod +x demodata-linux-amd64
./demodata-linux-amd64 -help
```

### Compilation depuis les sources

Prérequis : Go 1.21+, GCC, bibliothèques de développement X11/OpenGL (Linux)

```bash
# Linux
sudo apt-get install gcc libgl1-mesa-dev libx11-dev libxrandr-dev \
  libxinerama-dev libxcursor-dev libxi-dev pkg-config

git clone https://github.com/triboulin/demodata
cd demodata
go build -o demodata ./cmd/demodata
```

---

## Utilisation rapide

```bash
# Transformer un fichier CSV avec des règles YAML
demodata -input data.csv -config rules.yaml -output data_safe.csv

# Source depuis une base MySQL, réécriture en place
demodata -driver mysql -dsn "user:pass@tcp(localhost:3306)/mydb" -config rules.yaml

# Échantillon de 30%, reproductible
demodata -input data.csv -config rules.yaml -sample 30 -seed 42 -output sample.csv

# Lancer l'interface graphique
demodata -ui
```

---

## Options CLI

| Flag | Description | Exemple |
|------|-------------|---------|
| `-input` | Fichier source (CSV/JSON/XLSX) | `-input data.csv` |
| `-output` | Fichier de sortie | `-output data_safe.csv` |
| `-config` | Fichier de règles YAML/JSON | `-config rules.yaml` |
| `-seed` | Graine aléatoire pour la reproductibilité (défaut : 0) | `-seed 42` |
| `-driver` | Driver de BDD : `sqlite`, `mysql`, `pgx`, `postgres` | `-driver mysql` |
| `-dsn` | Chaîne de connexion à la base de données | `-dsn "user:pass@tcp(...)/"` |
| `-sample` | Taux d'échantillonnage en % (1–100, défaut : 100) | `-sample 25` |
| `-ui` | Lancer l'interface graphique | `-ui` |
| `-help` | Afficher l'aide | |

### Chaînes de connexion (DSN)

```bash
# SQLite
-driver sqlite -dsn /path/to/file.db
-driver sqlite -dsn :memory:

# MySQL
-driver mysql -dsn "user:password@tcp(localhost:3306)/dbname"

# PostgreSQL
-driver pgx -dsn "postgres://user:password@localhost:5432/dbname"
```

---

## Formats supportés

### Entrée

| Format | Notes |
|--------|-------|
| **CSV** | Première ligne = en-têtes, détection automatique des types |
| **JSON** | Tableau d'objets `[{...}, {...}]` |
| **XLSX** | Chaque feuille devient une table distincte |
| **SQLite** | Toutes les tables chargées automatiquement |
| **MySQL** | Toutes les tables chargées automatiquement |
| **PostgreSQL** | Toutes les tables chargées automatiquement |

### Sortie

| Format | Notes |
|--------|-------|
| **CSV** | Un fichier par table si jeu de données multi-tables |
| **JSON** | Indenté à 2 espaces, un fichier par table |
| **XLSX** | Toutes les tables dans un seul classeur (feuilles séparées) |
| **Base de données** | Suppression + réinsertion atomique, FK désactivées pendant l'opération |

---

## Règles de transformation

Les règles sont définies dans un fichier YAML ou JSON. Chaque table peut recevoir plusieurs transformateurs appliqués dans l'ordre.

### Structure du fichier de config

```yaml
tables:
  - name: nom_de_la_table
    transformers:
      - name: identifiant_unique   # Label libre (affiché dans l'UI)
        type: masker               # Type de transformation
        options:
          # Options spécifiques au type
```

> Pour les fichiers CSV/JSON/XLSX à table unique, utiliser le nom de fichier sans extension comme nom de table (ex : `data` pour `data.csv`).

---

### Masker — Masquage

Remplace chaque caractère d'une valeur par un caractère de masquage. La longueur de la valeur est préservée.

```yaml
- name: masquer_email
  type: masker
  options:
    column_name: email
    mask_char: "*"     # Caractère de remplacement (défaut : "*")
```

**Exemple :** `"john@example.com"` → `"****************"`

---

### Shuffler — Mélange

Redistribue aléatoirement les valeurs d'une ou plusieurs colonnes entre les lignes. En cas de colonnes multiples, les tuples sont déplacés ensemble (cohérence préservée).

```yaml
- name: melanger_noms
  type: shuffler
  options:
    column_names:
      - first_name
      - last_name
```

**Exemple :** les valeurs `(Alice, Martin)` et `(Bob, Dupont)` peuvent devenir `(Bob, Dupont)` et `(Alice, Martin)` — les paires restent intactes.

---

### Generator — Génération

Génère de nouvelles valeurs selon un type ou un pattern regex.

```yaml
- name: generer_id
  type: generator
  options:
    column_name: reference
    data_type: string          # integer | float | boolean | string
    format: "^[A-Z]{2}[0-9]{4}$"  # Optionnel : regex pour la forme des valeurs
```

| `data_type` | Valeur générée |
|-------------|----------------|
| `integer` | Entier aléatoire |
| `float` | Flottant aléatoire |
| `boolean` | `true` ou `false` |
| `string` | Chaîne alphanumérique (10 chars, ou selon `format`) |

Le champ `format` accepte un regex (ex : `^[A-Z]{2}[0-9]{3}$` → `"AZ123"`). Si la génération par regex échoue, le type de base est utilisé en fallback.

---

### None — Pas de transformation

Conserve les données telles quelles. Utilisé comme placeholder dans les configs générées par l'inféreur.

```yaml
- name: conserver_date
  type: none
```

---

## Exemple complet

```yaml
# rules.yaml — Anonymisation d'une base clients/commandes

tables:
  - name: users
    transformers:
      - name: masquer_email
        type: masker
        options:
          column_name: email
          mask_char: "*"

      - name: masquer_telephone
        type: masker
        options:
          column_name: phone
          mask_char: "*"

      - name: melanger_noms
        type: shuffler
        options:
          column_names:
            - first_name
            - last_name

      - name: melanger_ville
        type: shuffler
        options:
          column_names:
            - city
            - postal_code

  - name: orders
    transformers:
      - name: masquer_montant
        type: masker
        options:
          column_name: amount
          mask_char: "0"
```

```bash
demodata -driver mysql -dsn "demo:demopass@tcp(localhost:3306)/testdb" \
         -config rules.yaml -seed 42
```

---

## Inférence automatique de règles

L'inféreur analyse le jeu de données et génère un fichier de config prêt à personnaliser.

```bash
# Générer un fichier de règles depuis un CSV
demodata -input data.csv -config inferred_rules.yaml
```

L'inféreur détecte automatiquement :

| Sémantique | Colonnes reconnues |
|-----------|-------------------|
| `email` | email, mail, courriel |
| `phone` | phone, tel, mobile |
| `iban` | iban |
| `credit_card` | card, creditcard, pan |
| `ip` | ip, ip_address |
| `uuid` | uuid, guid |
| `zip` | zip, postal |
| `date` | date, birthdate, naissance |
| `first_name` / `last_name` | prénom, nom, firstname… |
| `address` / `city` / `country` | adresse, ville, pays… |
| `secret` / `password` / `token` | mot de passe, secret, clé… |
| `ssn` | ssn, secu, numero_secu |

Le fichier généré contient pour chaque colonne :
- le type de données détecté (`integer`, `float`, `boolean`, `string`)
- le pattern regex inféré depuis les valeurs (ex : `^[0-9]{5}$`)
- la sémantique détectée
- les statistiques pour les colonnes numériques (`min`, `max`, `avg`, `std`)
- le transformateur `none` comme point de départ (à remplacer)

---

## Échantillonnage

Le flag `-sample` permet de ne traiter qu'un sous-ensemble des données, utile pour créer des jeux de données de développement légers.

```bash
demodata -driver pgx -dsn "postgres://..." -config rules.yaml -sample 20 -seed 42
```

- Le taux est exprimé en pourcentage (1–100)
- La sélection est aléatoire, contrôlée par `-seed`
- Avec une source base de données, les violations de clés étrangères sont automatiquement filtrées : si un enregistrement parent n'est pas retenu dans l'échantillon, les enfants orphelins sont supprimés

---

## Interface graphique

L'interface native (Fyne) se lance avec :

```bash
demodata -ui
# ou en pré-chargeant un fichier
demodata -ui -input data.csv
```

### Fonctionnalités

- **Import** par drag-and-drop ou sélecteur de fichiers (CSV, JSON, XLSX)
- **Connexion base de données** : sélection du driver, saisie du DSN, chargement automatique des tables
- **Aperçu des règles** : liste de tous les transformateurs par table
- **Éditeur de règle** : modification du type (none / masker / shuffler / generator) et des options associées
- **Curseur d'échantillonnage** (1–100%)
- **Bouton d'exécution** : applique les règles et exporte
- **Barre de statut** : retour en temps réel

---

## Structure du projet

```
demodata/
├── cmd/demodata/main.go        # Point d'entrée CLI
├── pkg/
│   ├── config/config.go        # Parsing YAML/JSON des règles
│   ├── ingest/
│   │   ├── ingest.go           # Lecture CSV/JSON/XLSX
│   │   └── db.go               # Lecture depuis BDD + détection FK
│   ├── inferer/inferer.go      # Analyse automatique du jeu de données
│   ├── transform/transform.go  # Moteur de transformation (masker/shuffler/generator)
│   ├── export/
│   │   ├── export.go           # Export CSV/JSON/XLSX
│   │   └── db.go               # Écriture en base (transactionnel)
│   └── ui/ui.go                # Interface graphique Fyne
├── test/
│   ├── example_data.csv
│   ├── rules/
│   │   ├── mysql_rules.yaml
│   │   └── postgres_rules.yaml
│   └── db/                     # Scripts SQL pour les tests d'intégration
└── .github/workflows/
    └── release.yml             # Pipeline CI/CD → releases Linux + Windows
```

---

## Tests

```bash
# Tous les tests
go test ./...

# Tests d'intégration (nécessite Docker)
docker compose up -d
go test ./pkg/ingest/... ./pkg/export/... -run TestDB
docker compose down
```

---

## Licence

MIT
