#!/usr/bin/env bash
# Vérifie que les alertes se déclenchent pour de vrai (lancé par la CI après up.sh) :
#   1. un DAG en échec  -> AirflowDagEnEchec, transmise à Alertmanager
#   2. une requête lente -> TrinoRequetesLentes
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
cd "$ROOT_DIR"

prometheus() { curl -s -G "http://localhost:30090/api/v1/query" --data-urlencode "query=$1"; }

attendre_alerte() { # attendre_alerte <nom> <filtre PromQL> <délai en secondes>
  local fin=$((SECONDS + $3))
  while [ "$SECONDS" -lt "$fin" ]; do
    if prometheus "ALERTS{alertname=\"$1\",alertstate=\"firing\"$2}" | grep -q '"value"'; then return 0; fi
    printf '.'
    sleep 15
  done
  return 1
}

step "Alerte « DAG en échec »"
EXPECT_STATE=failed "$ROOT_DIR/scripts/run-dag.sh" ingestion_ftth '{"simuler_echec": true}'
attendre_alerte AirflowDagEnEchec ',dag_id="ingestion_ftth"' 300 || die "AirflowDagEnEchec ne s'est pas déclenchée"
echo
ok "AirflowDagEnEchec active dans Prometheus"
for _ in $(seq 1 12); do
  curl -s http://localhost:30093/api/v2/alerts | grep -q '"alertname":"AirflowDagEnEchec"' && break
  sleep 10
done
curl -s http://localhost:30093/api/v2/alerts | grep -q '"alertname":"AirflowDagEnEchec"' \
  || die "l'alerte n'est pas arrivée dans Alertmanager"
ok "alerte reçue par Alertmanager"

step "Alerte « requête lente »"
# Le seuil est abaissé à 2 s le temps du test, pour ne pas dépendre de la puissance de la machine.
hm upgrade trino-usage charts/trino-usage -n trino --reuse-values --set slowQuerySeconds=2 --wait --timeout 3m >/dev/null
trino_sql "SELECT count_if(regexp_like(l.comment, '(fur|ly).*(ironic|even)')) FROM tpch.sf1.lineitem l CROSS JOIN UNNEST(sequence(1, 3))" analyste >/dev/null
attendre_alerte TrinoRequetesLentes ',user="analyste",catalog="tpch"' 180 || die "TrinoRequetesLentes ne s'est pas déclenchée"
echo
ok "TrinoRequetesLentes active pour l'utilisateur analyste sur le catalogue tpch"
hm upgrade trino-usage charts/trino-usage -n trino --reuse-values --set slowQuerySeconds=10 --wait --timeout 3m >/dev/null
ok "seuil remis à 10 s"

step "Retour à un état sain"
"$ROOT_DIR/scripts/run-dag.sh" ingestion_ftth
