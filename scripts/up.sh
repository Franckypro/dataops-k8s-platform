#!/usr/bin/env bash
# Lance toute la plateforme en une commande (le même script sert en local et dans la CI).
#   ./up.sh            depuis Git Bash, WSL, Linux ou macOS
#   .\up.ps1           depuis PowerShell (appelle ce script avec Git Bash)
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
cd "$ROOT_DIR"

step "1/9 Prérequis"
require_cmd docker kind helm kubectl curl
docker info >/dev/null 2>&1 || die "Docker ne répond pas : démarrez Docker Desktop puis relancez."
memoire_docker=$(docker info --format '{{.MemTotal}}')
if [ "$memoire_docker" -lt $((6 * 1024 * 1024 * 1024)) ]; then
  warn "Docker dispose de $((memoire_docker / 1024 / 1024)) Mio : la plateforme complète en demande environ 5 Gio."
fi
ok "docker, kind, helm, kubectl, curl"

step "2/9 Cluster Kubernetes local (kind)"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  ok "le cluster $CLUSTER_NAME existe déjà"
else
  kind create cluster --config kind/cluster.yaml
fi
kc cluster-info >/dev/null && ok "contexte $KUBE_CONTEXT"

step "3/9 Images de la plateforme"
docker build -q -t "$EXPORTER_IMAGE" images/trino-usage-exporter >/dev/null && ok "image $EXPORTER_IMAGE"
docker build -q -t "$AIRFLOW_IMAGE" -f images/airflow/Dockerfile . >/dev/null && ok "image $AIRFLOW_IMAGE"
kind load docker-image "$EXPORTER_IMAGE" "$AIRFLOW_IMAGE" --name "$CLUSTER_NAME" >/dev/null
ok "images chargées dans le nœud kind"

step "4/9 Dépôts Helm et secrets"
helm repo add trino https://trinodb.github.io/charts --force-update >/dev/null
helm repo add apache-airflow https://airflow.apache.org --force-update >/dev/null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update >/dev/null
helm repo add opencost https://opencost.github.io/opencost-helm-chart --force-update >/dev/null
helm repo update trino apache-airflow prometheus-community opencost >/dev/null
ensure_secrets_file
for ns in monitoring lakehouse trino airflow opencost; do ensure_namespace "$ns"; done
apply_secret monitoring grafana-admin "admin-user=admin" "admin-password=$GRAFANA_ADMIN_PASSWORD"
apply_secret lakehouse minio-credentials "root-user=$MINIO_ROOT_USER" "root-password=$MINIO_ROOT_PASSWORD"
apply_secret trino trino-s3 "AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER" "AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD"
apply_secret airflow airflow-s3 "AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER" "AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD"
apply_secret airflow airflow-db "password=$AIRFLOW_DB_PASSWORD"
apply_secret airflow airflow-metadata \
  "connection=postgresql://airflow:${AIRFLOW_DB_PASSWORD}@postgres.airflow.svc.cluster.local:5432/airflow?sslmode=disable"
ok "secrets Kubernetes à jour (valeurs dans .secrets/platform.env)"

step "5/9 Supervision : Prometheus, Alertmanager, Grafana"
hm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --version "$KPS_CHART_VERSION" -n monitoring -f values/kube-prometheus-stack.yaml --wait --timeout 10m >/dev/null
ok "kube-prometheus-stack $KPS_CHART_VERSION"

step "6/9 Lakehouse : MinIO, catalogue Iceberg, Trino"
hm upgrade --install lakehouse charts/lakehouse -n lakehouse --wait --timeout 5m >/dev/null
ok "MinIO et catalogue REST Iceberg"
hm upgrade --install trino-usage charts/trino-usage -n trino --set "image.tag=$EXPORTER_TAG" --wait --timeout 3m >/dev/null
ok "collecteur d'usage Trino"
# Une montée de version déjà faite (scripts/trino-upgrade.sh) n'est pas annulée par une relance de up.sh
version_trino="$TRINO_CHART_VERSION"
installee=$(hm -n trino list --filter '^trino$' -o json 2>/dev/null | sed -n 's/.*"chart":"trino-\([0-9.]*\)".*/\1/p')
if [ -n "$installee" ] && [ "$installee" != "$TRINO_CHART_VERSION" ]; then version_trino="$installee"; fi
hm upgrade --install trino trino/trino --version "$version_trino" -n trino -f values/trino.yaml --wait --timeout 10m >/dev/null
ok "Trino (chart $version_trino) : $(trino_sql 'SELECT version()' | tail -1)"

step "7/9 Orchestration : Airflow"
hm upgrade --install airflow-db charts/postgres -n airflow --wait --timeout 5m >/dev/null
ok "PostgreSQL des métadonnées Airflow"
hm upgrade --install airflow apache-airflow/airflow --version "$AIRFLOW_CHART_VERSION" -n airflow \
  -f values/airflow.yaml --set "images.airflow.tag=$AIRFLOW_TAG" \
  --set "createUserJob.defaultUser.password=$AIRFLOW_ADMIN_PASSWORD" --wait --timeout 15m >/dev/null
ok "Airflow (chart $AIRFLOW_CHART_VERSION)"

step "8/9 Coûts et tableaux de bord : OpenCost, alertes, Grafana"
hm upgrade --install opencost opencost/opencost --version "$OPENCOST_CHART_VERSION" -n opencost \
  -f values/opencost.yaml --wait --timeout 5m >/dev/null
ok "OpenCost $OPENCOST_CHART_VERSION"
hm upgrade --install supervision charts/supervision -n monitoring --wait --timeout 3m >/dev/null
ok "règles d'alerte, ServiceMonitor et tableaux Grafana"

step "9/9 Premier chargement et vérifications"
"$ROOT_DIR/scripts/run-dag.sh" --attendre ingestion_ftth
AIRFLOW_JETON=$(airflow_jeton); export AIRFLOW_JETON
airflow_api PATCH "dags/controle_sla_data_products" '{"is_paused": false}' >/dev/null
ok "contrôle de niveau de service activé (toutes les heures)"
"$ROOT_DIR/scripts/smoke-test.sh"

cat <<EOF

${C_GREEN}Plateforme prête.${C_OFF}
  Airflow      http://localhost:30080   (admin / AIRFLOW_ADMIN_PASSWORD)
  Grafana      http://localhost:30030   (admin / GRAFANA_ADMIN_PASSWORD), dossier « DataOps »
  Trino        http://localhost:30088   (interface /ui, n'importe quel nom d'utilisateur)
  Prometheus   http://localhost:30090
  Alertmanager http://localhost:30093
  OpenCost     http://localhost:30091
  MinIO        http://localhost:30901   (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)
Mots de passe : .secrets/platform.env
EOF
