#!/usr/bin/env bash
# Supprime le cluster kind et tout ce qu'il contient (données comprises).
# Les mots de passe de .secrets/ sont conservés pour le prochain lancement.
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

step "Suppression du cluster $CLUSTER_NAME"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  kind delete cluster --name "$CLUSTER_NAME"
  ok "cluster supprimé"
else
  ok "aucun cluster $CLUSTER_NAME à supprimer"
fi
