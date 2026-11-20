#!/usr/bin/env bash
# Déclenche un DAG Airflow par l'API REST et attend la fin de l'exécution.
#   scripts/run-dag.sh ingestion_ftth
#   EXPECT_STATE=failed scripts/run-dag.sh ingestion_ftth '{"simuler_echec": true}'
#   scripts/run-dag.sh --attendre ingestion_ftth
#       attend la dernière exécution existante au lieu d'en créer une : au premier démarrage, Airflow
#       lance de lui-même la dernière échéance planifiée (catchup=False), inutile d'en ajouter une.
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
ensure_secrets_file

attendre_existante=0
if [ "${1:-}" = "--attendre" ]; then attendre_existante=1; shift; fi
dag_id="${1:?usage : run-dag.sh [--attendre] <dag_id> [conf JSON]}"
conf="${2:-}"
[ -n "$conf" ] || conf='{}'
attendu="${EXPECT_STATE:-success}"

for _ in $(seq 1 60); do
  AIRFLOW_JETON=$(airflow_jeton || true)
  [ -n "$AIRFLOW_JETON" ] && airflow_api GET "dags/$dag_id" >/dev/null 2>&1 && break
  sleep 5
done
[ -n "${AIRFLOW_JETON:-}" ] || die "l'API Airflow ($AIRFLOW_URL) ne répond pas"
airflow_api GET "dags/$dag_id" >/dev/null || die "DAG $dag_id introuvable dans Airflow"

derniere_execution() { airflow_api GET "dags/$dag_id/dagRuns?order_by=-run_after&limit=1" | json_champ dag_run_id; }
etat() { airflow_api GET "dags/$dag_id/dagRuns/$1" | json_champ state; }

run_id=""
if [ "$attendre_existante" = 1 ]; then
  for _ in $(seq 1 12); do
    run_id=$(derniere_execution || true)
    [ -n "$run_id" ] && break
    sleep 5
  done
  # Une exécution déjà en échec ne sert à rien : on en lance une nouvelle
  if [ -n "$run_id" ] && [ "$(etat "$run_id" || true)" = "failed" ]; then run_id=""; fi
fi
if [ -n "$run_id" ]; then
  printf "  attente de l'exécution %s du DAG %s" "$run_id" "$dag_id"
else
  run_id="manuel__$(date -u +%Y%m%dT%H%M%S)"
  airflow_api POST "dags/$dag_id/dagRuns" \
    "{\"dag_run_id\": \"$run_id\", \"logical_date\": null, \"conf\": $conf}" >/dev/null \
    || die "impossible de déclencher $dag_id"
  printf '  exécution %s du DAG %s lancée' "$run_id" "$dag_id"
fi

resultat=""
for _ in $(seq 1 120); do
  resultat=$(etat "$run_id" || true)
  case "$resultat" in success|failed) break ;; esac
  printf '.'
  sleep 10
done
echo

if [ "$resultat" = "$attendu" ]; then
  ok "$dag_id : $resultat (attendu)"
else
  airflow_api GET "dags/$dag_id/dagRuns/$run_id/taskInstances" \
    | grep -o '"task_id":"[^"]*"\|"state":"[^"]*"' | paste - - || true
  die "$dag_id : état « ${resultat:-inconnu} », attendu « $attendu »"
fi
