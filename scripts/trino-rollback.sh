#!/usr/bin/env bash
# Retour arrière de Trino vers une révision Helm précédente (la précédente par défaut).
#   scripts/trino-rollback.sh        -> révision précédente
#   scripts/trino-rollback.sh 3      -> révision 3 (voir « helm history trino -n trino »)
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

revision="${1:-}"
version_avant=$(trino_sql "SELECT version()" | tail -1)
step "Retour arrière de Trino (version actuelle : $version_avant)"
if [ -n "$revision" ]; then
  hm rollback trino "$revision" -n trino --wait --timeout 10m
else
  hm rollback trino -n trino --wait --timeout 10m
fi
version_apres=$(trino_sql "SELECT version()" | tail -1)
nb=$(trino_sql "SELECT count(*) FROM iceberg.reseau_fixe.ftth_communes" | tail -1)
ok "Trino est revenu en version $version_apres ; les tables Iceberg sont intactes ($nb lignes dans ftth_communes)"
hm -n trino history trino --max 5
