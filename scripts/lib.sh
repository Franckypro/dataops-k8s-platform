#!/usr/bin/env bash
# Fonctions communes aux scripts de la plateforme (lancement local et CI).
# Les variables définies ici sont utilisées par les scripts qui chargent ce fichier.
# shellcheck disable=SC2034

# Git Bash (Windows) convertit les arguments qui commencent par "/" en chemins Windows : on le désactive.
export MSYS_NO_PATHCONV=1

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-dataops}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
SECRETS_FILE="${ROOT_DIR}/.secrets/platform.env"

# Versions déployées (une seule source de vérité, reprise par la CI)
TRINO_CHART_VERSION="${TRINO_CHART_VERSION:-1.44.0}"            # Trino 483
TRINO_UPGRADE_CHART_VERSION="${TRINO_UPGRADE_CHART_VERSION:-1.45.0}" # Trino 484
AIRFLOW_CHART_VERSION="${AIRFLOW_CHART_VERSION:-1.22.0}"        # Airflow 3.2.2
KPS_CHART_VERSION="${KPS_CHART_VERSION:-93.5.0}"                # kube-prometheus-stack
OPENCOST_CHART_VERSION="${OPENCOST_CHART_VERSION:-2.5.34}"      # OpenCost 1.121.4

# Étiquette d'image = empreinte de son contenu : toute modification des DAG, des contrats ou du code
# produit une nouvelle image, et Helm redéploie les pods concernés.
contenu_hash() {
  (cd "$ROOT_DIR" && find "$@" -type f -not -path '*/__pycache__/*' -not -path '*/tests/*' -not -name '*.pyc' \
    | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-12)
}
AIRFLOW_TAG="${AIRFLOW_TAG:-$(contenu_hash images/airflow dags data-products)}"
EXPORTER_TAG="${EXPORTER_TAG:-$(contenu_hash images/trino-usage-exporter)}"
AIRFLOW_IMAGE="dataops/airflow:${AIRFLOW_TAG}"
EXPORTER_IMAGE="dataops/trino-usage-exporter:${EXPORTER_TAG}"

if [ -t 1 ]; then
  C_BLUE=$'\033[1;34m'; C_GREEN=$'\033[1;32m'; C_YELLOW=$'\033[1;33m'; C_RED=$'\033[1;31m'; C_OFF=$'\033[0m'
else
  C_BLUE=""; C_GREEN=""; C_YELLOW=""; C_RED=""; C_OFF=""
fi

step() { printf '\n%s==> %s%s\n' "$C_BLUE" "$*" "$C_OFF"; }
ok()   { printf '%s  ok%s %s\n' "$C_GREEN" "$C_OFF" "$*"; }
warn() { printf '%s  attention%s %s\n' "$C_YELLOW" "$C_OFF" "$*"; }
die()  { printf '%s  erreur%s %s\n' "$C_RED" "$C_OFF" "$*" >&2; exit 1; }

kc()   { kubectl --context "$KUBE_CONTEXT" "$@"; }
hm()   { helm --kube-context "$KUBE_CONTEXT" "$@"; }

require_cmd() {
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "commande introuvable : $c (voir la section Prérequis du README)"
  done
}

random_secret() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
  else
    python3 -c 'import secrets; print(secrets.token_hex(16))' 2>/dev/null || python -c 'import secrets; print(secrets.token_hex(16))'
  fi
}

# Les mots de passe sont générés au premier lancement dans .secrets/ (ignoré par git), puis réutilisés.
ensure_secrets_file() {
  if [ ! -f "$SECRETS_FILE" ]; then
    mkdir -p "$(dirname "$SECRETS_FILE")"
    {
      echo "MINIO_ROOT_USER=platform-admin"
      echo "MINIO_ROOT_PASSWORD=$(random_secret)"
      echo "AIRFLOW_DB_PASSWORD=$(random_secret)"
      echo "AIRFLOW_ADMIN_PASSWORD=$(random_secret)"
      echo "GRAFANA_ADMIN_PASSWORD=$(random_secret)"
    } > "$SECRETS_FILE"
    ok "mots de passe générés dans .secrets/platform.env"
  fi
  set -a
  # shellcheck source=/dev/null
  . "$SECRETS_FILE"
  set +a
}

ensure_namespace() {
  kc create namespace "$1" --dry-run=client -o yaml | kc apply -f - >/dev/null
}

# apply_secret <namespace> <nom> clé=valeur [clé=valeur ...]
apply_secret() {
  local ns="$1" name="$2"; shift 2
  local args=()
  for kv in "$@"; do args+=("--from-literal=$kv"); done
  kc -n "$ns" create secret generic "$name" "${args[@]}" --dry-run=client -o yaml | kc apply -f - >/dev/null
}

# Exécute une requête SQL avec le client trino du pod coordinateur. Utilisateur par défaut : « plateforme ».
#   trino_sql "SELECT 1"            trino_sql "SELECT 1" analyste
trino_sql() {
  kc -n trino exec deploy/trino-coordinator -- trino --user "${2:-plateforme}" --output-format=TSV --execute "$1"
}

# API REST d'Airflow 3, exposée sur le NodePort 30080. On évite la CLI « airflow » dans le pod du
# scheduler : elle consomme la mémoire réservée au scheduler et à ses tâches.
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:30080}"

airflow_jeton() {
  curl -sf -m 60 -X POST "$AIRFLOW_URL/auth/token" -H 'Content-Type: application/json' \
    -d "{\"username\":\"admin\",\"password\":\"$AIRFLOW_ADMIN_PASSWORD\"}" \
    | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'
}

# airflow_api <MÉTHODE> <chemin sous /api/v2> [corps JSON] ; nécessite AIRFLOW_JETON
airflow_api() {
  local methode="$1" chemin="$2" corps="${3:-}"
  local args=(-sf -m 60 -X "$methode" "$AIRFLOW_URL/api/v2/$chemin" -H "Authorization: Bearer ${AIRFLOW_JETON:-}")
  if [ -n "$corps" ]; then args+=(-H 'Content-Type: application/json' -d "$corps"); fi
  curl "${args[@]}"
}

# Valeur d'un champ texte de premier niveau dans une réponse JSON compacte
json_champ() { sed -n "s/.*\"$1\":\"\\([^\"]*\\)\".*/\\1/p" | head -1; }
