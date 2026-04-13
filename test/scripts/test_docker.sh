#!/usr/bin/env bash
# test/scripts/test_docker.sh
# Lance les conteneurs Docker, attend qu'ils soient prêts, compile le binaire,
# puis vérifie l'anonymisation CLI sur MySQL et PostgreSQL.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_ROOT/bin/demodata"

cd "$PROJECT_ROOT"

# Arrêter les conteneurs à la sortie (succès ou échec)
teardown() {
  echo ""
  echo "==> Arrêt des conteneurs Docker"
  docker compose down 2>/dev/null || docker-compose down
}
trap teardown EXIT

# ── Compilation ────────────────────────────────────────────────────────────────
echo "==> Compilation du binaire"
mkdir -p bin
go build -o "$BIN" ./cmd/demodata/

# ── Démarrage des conteneurs ────────────────────────────────────────────────────
echo "==> Démarrage des conteneurs Docker"
docker compose up -d --wait 2>/dev/null || docker-compose up -d

wait_healthy() {
  local service="$1"
  local max=60
  echo "==> Attente du service $service (max ${max}s)…"
  for i in $(seq 1 $max); do
    status=$(docker inspect --format='{{.State.Health.Status}}' "demodata-$service" 2>/dev/null || echo "unknown")
    if [[ "$status" == "healthy" ]]; then
      echo "    $service OK"
      return 0
    fi
    sleep 1
  done
  echo "ERREUR : $service non disponible après ${max}s"
  docker logs "demodata-$service" | tail -20
  exit 1
}

wait_healthy mysql
wait_healthy postgres

# ── Petite pause pour que les scripts init finissent ──────────────────────────
sleep 3

# ══════════════════════════════════════════════════════════════════════════════
# TEST MySQL
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════"
echo "  TEST MYSQL"
echo "══════════════════════════════════════"

MYSQL_DSN="demo:demopass@tcp(localhost:3306)/testdb"
MYSQL_RULES="$PROJECT_ROOT/test/rules/mysql_rules.yaml"

echo "--- Données originales (email colonne 4 des 5 premiers users) ---"
docker exec demodata-mysql mysql -u demo -pdemopass testdb \
  -e "SELECT id, first_name, last_name, email, phone FROM users LIMIT 5;" 2>/dev/null

echo ""
echo "--- Anonymisation MySQL ---"
"$BIN" -driver mysql -dsn "$MYSQL_DSN" -config "$MYSQL_RULES" -seed 42

echo ""
echo "--- Données après anonymisation (email devrait être masqué) ---"
docker exec demodata-mysql mysql -u demo -pdemopass testdb \
  -e "SELECT id, first_name, last_name, email, phone FROM users LIMIT 5;" 2>/dev/null

echo ""
echo "--- Vérification : l'email ne contient plus '@' ---"
COUNT=$(docker exec demodata-mysql mysql -u demo -pdemopass testdb \
  -Ne "SELECT COUNT(*) FROM users WHERE email LIKE '%@%';" 2>/dev/null | tr -d '[:space:]')
if [[ "$COUNT" == "0" ]]; then
  echo "✓ MySQL : emails correctement masqués (0 email intact)"
else
  echo "✗ MySQL : $COUNT email(s) avec '@' encore présent(s) !"
  exit 1
fi

# ══════════════════════════════════════════════════════════════════════════════
# TEST PostgreSQL
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════"
echo "  TEST POSTGRESQL"
echo "══════════════════════════════════════"

PG_DSN="postgres://demo:demopass@localhost:5432/testdb"
PG_RULES="$PROJECT_ROOT/test/rules/postgres_rules.yaml"

echo "--- Données originales ---"
docker exec demodata-postgres psql -U demo testdb \
  -c "SELECT id, first_name, last_name, email, phone FROM users LIMIT 5;" 2>/dev/null

echo ""
echo "--- Anonymisation PostgreSQL ---"
"$BIN" -driver pgx -dsn "$PG_DSN" -config "$PG_RULES" -seed 42

echo ""
echo "--- Données après anonymisation ---"
docker exec demodata-postgres psql -U demo testdb \
  -c "SELECT id, first_name, last_name, email, phone FROM users LIMIT 5;" 2>/dev/null

echo ""
echo "--- Vérification : l'email ne contient plus '@' ---"
COUNT=$(docker exec demodata-postgres psql -U demo testdb -At \
  -c "SELECT COUNT(*) FROM users WHERE email LIKE '%@%';" 2>/dev/null | tr -d '[:space:]')
if [[ "$COUNT" == "0" ]]; then
  echo "✓ PostgreSQL : emails correctement masqués (0 email intact)"
else
  echo "✗ PostgreSQL : $COUNT email(s) avec '@' encore présent(s) !"
  exit 1
fi

# ══════════════════════════════════════════════════════════════════════════════
# TEST reproductibilité (même seed → même résultat)
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════"
echo "  TEST REPRODUCTIBILITÉ"
echo "══════════════════════════════════════"
reset_mysql() {
  docker exec demodata-mysql mysql -u demo -pdemopass testdb \
    -e "SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE orders; TRUNCATE TABLE users; SET FOREIGN_KEY_CHECKS=1;" 2>/dev/null
  docker exec -i demodata-mysql mysql -u demo -pdemopass testdb \
    < "$PROJECT_ROOT/test/db/mysql/02_data.sql" 2>/dev/null
  echo "    DB réinitialisée ($(docker exec demodata-mysql mysql -u demo -pdemopass testdb \
    -Ne 'SELECT COUNT(*) FROM users;' 2>/dev/null | tr -d '[:space:]') users)"
}

echo "--- Reset MySQL + passe A avec seed=42 ---"
reset_mysql
"$BIN" -driver mysql -dsn "$MYSQL_DSN" -config "$MYSQL_RULES" -seed 42

FIRST_NAME_1=$(docker exec demodata-mysql mysql -u demo -pdemopass testdb \
  -Ne "SELECT first_name FROM users ORDER BY id LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
echo "    → first_name[1] = '$FIRST_NAME_1'"

echo "--- Reset MySQL + passe B avec seed=42 ---"
reset_mysql
"$BIN" -driver mysql -dsn "$MYSQL_DSN" -config "$MYSQL_RULES" -seed 42

FIRST_NAME_2=$(docker exec demodata-mysql mysql -u demo -pdemopass testdb \
  -Ne "SELECT first_name FROM users ORDER BY id LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
echo "    → first_name[1] = '$FIRST_NAME_2'"

if [[ "$FIRST_NAME_1" == "$FIRST_NAME_2" ]]; then
  echo "✓ Résultat reproductible avec le même seed (first_name = '$FIRST_NAME_1')"
else
  echo "✗ Résultats différents entre deux passes avec seed=42 !"
  exit 1
fi

echo ""
echo "══════════════════════════════════════"
echo "  TOUS LES TESTS RÉUSSIS"
echo "══════════════════════════════════════"
