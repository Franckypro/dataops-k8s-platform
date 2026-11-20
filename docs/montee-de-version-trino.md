# Montée de version de Trino : 483 → 484, avec retour arrière

Trino est déployé avec le chart officiel `trinodb/charts`. Chaque version du chart embarque une version de Trino :

| Chart `trino/trino` | Trino | Rôle dans ce dépôt |
|---|---|---|
| 1.44.0 | 483 | version installée par `up.sh` |
| 1.45.0 | 484 | version cible de la montée de version |

On change **la version du chart**, pas seulement l'image : le chart adapte aussi la configuration à la version de Trino
(voir les notes de version ci-dessous). Les valeurs propres à la plateforme restent dans `values/trino.yaml`.

## 1. Avant la montée de version

1. Lire les notes de version : <https://trino.io/docs/current/release/release-484.html> (Trino 484, 15 octobre 2026).
   Points relevés pour cette plateforme :
   - l'image Docker ne contient plus le script `run-trino` : un `node.properties` personnalisé doit définir
     `node.id=${ENV:HOSTNAME}` (le chart 1.45.0 s'en charge, c'est pourquoi on monte le chart et pas l'image seule) ;
   - les tables de métadonnées Iceberg `$snapshots`, `$history` et `$metadata_log_entries` renvoient des horodatages
     UTC avec fuseau : le DAG `controle_sla_data_products`, qui lit `$snapshots`, reste compatible ;
   - aucune propriété utilisée dans `values/trino.yaml` n'est retirée.
2. Comparer les valeurs par défaut des deux charts :
   ```bash
   helm show values trino/trino --version 1.44.0 > /tmp/a.yaml
   helm show values trino/trino --version 1.45.0 > /tmp/b.yaml
   diff /tmp/a.yaml /tmp/b.yaml   # ici : seule l'image OPA change
   ```
3. Vérifier que le rendu est valide avec nos valeurs (la CI le fait à chaque commit) :
   ```bash
   helm template trino trino/trino --version 1.45.0 -f values/trino.yaml > /dev/null
   ```
4. Noter la révision Helm actuelle : `helm history trino -n trino`.

## 2. Montée de version

```bash
./scripts/trino-upgrade.sh          # chart 1.45.0 par défaut, ou ./scripts/trino-upgrade.sh <version du chart>
```

Le script :

1. note la version de Trino et la révision Helm en service ;
2. lance `helm upgrade trino trino/trino --version 1.45.0 -f values/trino.yaml --wait` ;
3. vérifie que Trino répond dans la nouvelle version, que le catalogue `iceberg` est présent, que les tables
   `reseau_fixe.ftth_communes` et `reseau_fixe.ftth_departements` se lisent, et qu'une écriture Iceberg fonctionne
   (`iceberg.verification.ecriture`) ;
4. si une vérification échoue, revient **automatiquement** à la révision précédente avec `helm rollback`.

Pendant la montée de version, le coordinateur redémarre : Trino est indisponible environ une à deux minutes.
L'alerte `TrinoIndisponible` (2 minutes d'indisponibilité) peut se déclencher, et le tableau Grafana
« Plateforme DataOps » montre le changement de version.

## 3. Retour arrière

```bash
./scripts/trino-rollback.sh         # révision précédente
./scripts/trino-rollback.sh 3       # une révision précise (voir helm history trino -n trino)
```

Les données ne sont pas touchées par un retour arrière : elles sont dans MinIO et leurs métadonnées dans le catalogue
Iceberg, hors du déploiement de Trino. Le script vérifie après coup la version et le nombre de lignes de `ftth_communes`.

## 4. Ce qui est testé en continu

Le job « Déploiement complet sur kind » de la CI enchaîne à chaque commit : installation en 483, montée en 484 avec
vérifications, puis retour arrière en 483. Une montée de version qui casse la lecture des tables Iceberg fait échouer
la CI avant d'arriver sur un environnement partagé.

## 5. En production

- Plusieurs workers et une montée de version progressive des workers, ou deux clusters derrière
  [Trino Gateway](https://trinodb.github.io/trino-gateway/) pour basculer le trafic sans coupure (bleu/vert).
- Une fenêtre de maintenance annoncée aux équipes consommatrices, et un jeu de requêtes de non-régression
  représentatif de leurs usages (les requêtes les plus fréquentes du tableau « Usages Trino »).
- La même procédure, versionnée dans Git et exécutée par la CI/CD (GitOps avec Argo CD ou Flux, par exemple).
