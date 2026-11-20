#!/usr/bin/env bash
# Vérifie que la plateforme fonctionne de bout en bout (lancé par up.sh et par la CI).
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
ensure_secrets_file

echec=0
verifier() { # verifier "<description>" <commande...>
  local description="$1"; shift
  if "$@"; then ok "$description"; else warn "ÉCHEC : $description"; echec=1; fi
}
au_moins() { [ "${1:-0}" -ge "$2" ]; }

# Relance une vérification jusqu'à son succès ou la fin du délai. Juste après l'installation, Prometheus met
# une à deux minutes à prendre en compte les nouveaux ServiceMonitor et règles, et Grafana ses tableaux.
reessayer() { # reessayer <secondes> <commande...>
  local fin=$((SECONDS + $1)); shift
  until "$@"; do
    [ "$SECONDS" -ge "$fin" ] && return 1
    sleep 10
  done
}

# Appels HTTP internes au cluster via le proxy de l'API Kubernetes (pas de port-forward)
proxy() { kc get --raw "/api/v1/namespaces/$1/services/$2/proxy/$3"; }
cible_collectee() { proxy monitoring kube-prometheus-stack-prometheus:9090 "api/v1/query?query=up%7Bjob%3D%22$1%22%7D%3D%3D1" | grep -q '"value"'; }
regle_chargee() { proxy monitoring kube-prometheus-stack-prometheus:9090 api/v1/rules | grep -q "\"$1\""; }
usage_suivi() { proxy trino trino-usage-exporter:8000 metrics | grep -q "trino_queries_total{catalog=\"iceberg\".*user=\"$1\"}"; }
cout_par_namespace() { proxy opencost opencost:9003 "allocation/compute?window=1h&aggregate=namespace&accumulate=true" | grep -q "\"$1\""; }
tableau_grafana() { curl -s -u "admin:$GRAFANA_ADMIN_PASSWORD" "http://localhost:30030/api/search?tag=dataops" | grep -q "\"$1\""; }

step "Tests de fumée"

# 1. Tables Iceberg produites par le DAG, lues avec Trino
communes=$(trino_sql "SELECT count(*) FROM iceberg.reseau_fixe.ftth_communes" | tail -1)
verifier "Iceberg : $communes lignes dans reseau_fixe.ftth_communes" au_moins "$communes" 30000
departements=$(trino_sql "SELECT count(*) FROM iceberg.reseau_fixe.ftth_departements" | tail -1)
verifier "Iceberg : $departements lignes dans reseau_fixe.ftth_departements" au_moins "$departements" 100
controles=$(trino_sql "SELECT count(*) FROM iceberg.plateforme.controles_qualite" | tail -1)
verifier "Iceberg : $controles contrôles qualité historisés" au_moins "$controles" 1

# 2. Le schéma réel de la table correspond au contrat YAML du data product
colonnes_table=$(trino_sql "SELECT column_name FROM iceberg.information_schema.columns WHERE table_schema = 'reseau_fixe' AND table_name = 'ftth_communes' ORDER BY ordinal_position" | tr -d '\r' | paste -sd, -)
colonnes_contrat=$(sed -n 's/^ *- { name: \([a-z0-9_]*\),.*/\1/p' "$ROOT_DIR/data-products/reseau_fixe/ftth_communes.yaml" | paste -sd, -)
verifier "schéma de ftth_communes identique à son contrat" test "$colonnes_table" = "$colonnes_contrat"

# 3. Usages Trino suivis par utilisateur et par catalogue
verifier "requêtes de l'utilisateur airflow sur le catalogue iceberg comptées" reessayer 60 usage_suivi airflow

# 4. Prometheus collecte les composants de la plateforme
for job in trino-usage-exporter airflow-statsd minio opencost; do
  verifier "Prometheus collecte $job" reessayer 300 cible_collectee "$job"
done

# 5. Règles d'alerte chargées
for alerte in AirflowDagEnEchec AirflowTacheEnEchec TrinoRequetesLentes TrinoIndisponible MinioIndisponible; do
  verifier "règle d'alerte $alerte chargée" reessayer 300 regle_chargee "$alerte"
done

# 6. OpenCost ventile les coûts par namespace
for ns in trino airflow lakehouse; do
  verifier "OpenCost calcule le coût du namespace $ns" reessayer 300 cout_par_namespace "$ns"
done

# 7. Tableaux Grafana de la plateforme
for uid in dataops-plateforme dataops-usages-trino dataops-finops; do
  verifier "tableau Grafana $uid" reessayer 180 tableau_grafana "$uid"
done

[ "$echec" -eq 0 ] || die "au moins un test de fumée a échoué"
ok "tous les tests de fumée sont passés"
