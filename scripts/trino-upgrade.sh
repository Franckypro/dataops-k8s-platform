#!/usr/bin/env bash
# Montée de version de Trino avec Helm, vérification, et retour arrière automatique si la vérification échoue.
#   scripts/trino-upgrade.sh            -> chart 1.45.0 (Trino 484)
#   scripts/trino-upgrade.sh 1.44.0     -> n'importe quelle version du chart trino/trino
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
cd "$ROOT_DIR"

cible="${1:-$TRINO_UPGRADE_CHART_VERSION}"
revision_avant=$(hm -n trino history trino --max 1 -o json | sed -n 's/.*"revision":\([0-9]*\).*/\1/p')
version_avant=$(trino_sql "SELECT version()" | tail -1)

step "Montée de version de Trino : $version_avant (révision Helm $revision_avant) -> chart $cible"
helm repo update trino >/dev/null
# Notes de version à relire avant toute montée de version : https://trino.io/docs/current/release.html
hm upgrade trino trino/trino --version "$cible" -n trino -f values/trino.yaml --wait --timeout 10m
version_apres=$(trino_sql "SELECT version()" | tail -1)
ok "Trino répond en version $version_apres"

step "Vérifications de non-régression"
verifier_trino() {
  trino_sql "SHOW CATALOGS" | grep -qx iceberg &&
  trino_sql "SELECT count(*) FROM iceberg.reseau_fixe.ftth_communes" | tail -1 | grep -qE '^[1-9][0-9]*$' &&
  trino_sql "SELECT count(*) FROM iceberg.reseau_fixe.ftth_departements" | tail -1 | grep -qE '^[1-9][0-9]*$' &&
  trino_sql "CREATE SCHEMA IF NOT EXISTS iceberg.verification" >/dev/null &&
  trino_sql "CREATE OR REPLACE TABLE iceberg.verification.ecriture AS SELECT current_timestamp AS horodatage" >/dev/null &&
  trino_sql "SELECT count(*) FROM iceberg.verification.ecriture" | tail -1 | grep -qx 1
}
if verifier_trino; then
  ok "lecture des tables Iceberg, écriture et catalogues : OK"
else
  warn "vérification en échec : retour à la révision $revision_avant"
  hm rollback trino "$revision_avant" -n trino --wait --timeout 10m
  die "montée de version annulée, Trino est revenu en version $(trino_sql 'SELECT version()' | tail -1)"
fi

step "Historique Helm de Trino"
hm -n trino history trino --max 5
echo "Retour arrière manuel si besoin : scripts/trino-rollback.sh $revision_avant"
