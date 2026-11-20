# Data products : un contrat YAML par table

Chaque table publiée sur la plateforme est décrite par un contrat dans `data-products/<domaine>/<nom>.yaml`.
Le contrat est la source de vérité, à trois moments :

| Moment | Ce que fait le contrat | Code |
|---|---|---|
| Revue de code (CI) | Le YAML respecte le schéma JSON et des règles de cohérence | `scripts/validate_data_products.py` |
| Chargement (Airflow) | La table Iceberg est créée ou alignée sur le contrat : colonnes, types, commentaires, partitions | `dags/dataops/lakehouse.py` |
| Après chargement (Airflow) | Les contrôles qualité du contrat sont traduits en SQL et exécutés par Trino | `dags/dataops/contrats.py`, `dags/dataops/qualite.py` |
| Toutes les heures (Airflow) | La fraîcheur de la table est comparée au niveau de service du contrat | `dags/controle_sla.py` |

## Les data products de la plateforme

| Domaine | Data product | Produit par | Contenu |
|---|---|---|---|
| `reseau_fixe` | `ftth_communes` | `ingestion_ftth` | Fibre et fermeture du cuivre, par commune et par trimestre (source ANCT) |
| `reseau_fixe` | `ftth_departements` | `ingestion_ftth` | Agrégat par département, calculé par Trino |
| `plateforme` | `controles_qualite` | `ingestion_ftth` | Historique de tous les contrôles qualité exécutés |

Le domaine devient le namespace Iceberg (`iceberg.reseau_fixe.*`) : chaque domaine publie ses tables sous son nom,
comme dans une organisation en data mesh.

## Anatomie d'un contrat

```yaml
apiVersion: dataops/v1
kind: DataProduct
metadata:
  name: ftth_communes            # = nom du fichier et nom de la table
  domain: reseau_fixe            # = dossier et namespace Iceberg
  title: ...
  description: ...
  owner:
    team: domaine-reseau-fixe    # équipe propriétaire
    steward: Franckypro          # personne responsable
spec:
  table: iceberg.reseau_fixe.ftth_communes
  producedBy: ingestion_ftth     # DAG producteur (vérifié par la CI)
  source: { name, publisher, url, licence, frequency }
  partitioning: [trimestre]
  schema:                        # colonnes : nom, type, obligatoire, colonne source, description
    - { name: code_insee, type: string, required: true, source: insee_com, description: ... }
  sla:
    refresh: quotidienne à 06:00 UTC
    freshness_max_hours: 30      # au-delà, le DAG controle_sla_data_products échoue et l'alerte se déclenche
  quality:                       # contrôles exécutés après chaque chargement
    - { type: unique, columns: [id_zone] }
    - { type: accepted_values, column: categorie_zonage, values: [ZTD, AMII, RIP, AMEL, FP] }
    - { type: range, column: locaux, min: 0 }
    - { type: row_count_min, min: 30000 }
    - { type: expression, name: raccordables_coherents,
        condition: "locaux_raccordables_ftth IS NULL OR locaux_raccordables_ftth <= locaux",
        max_failure_ratio: 0.01, severity: warn }
  access:
    classification: public       # public, interne ou confidentiel
    consumers: [reseau_fixe.ftth_departements]
```

Types de contrôles : `not_null`, `unique`, `accepted_values`, `range`, `row_count_min`, `expression`.
Chaque colonne `required: true` ajoute automatiquement un contrôle « colonnes obligatoires renseignées ».
`severity: error` (par défaut) bloque la publication ; `warn` est signalé sans bloquer.
`max_failure_ratio` fixe une tolérance : ici, la source compte parfois plus de locaux raccordables que de locaux
de référence (143 lignes sur 35 507 au 2e trimestre 2026), ce qui reste sous 1 %.

## Ce que vérifie la CI

1. Le YAML respecte `data-products/_schema/data-product.schema.json` (champs obligatoires, types, valeurs permises).
2. Le fichier est rangé à `data-products/<domaine>/<nom>.yaml` et la table vaut `iceberg.<domaine>.<nom>`.
3. Pas de colonne en double ; partitions et contrôles portent sur des colonnes du schéma.
4. Le DAG `producedBy` existe dans `dags/`, et les consommateurs de la forme `domaine.nom` existent.
5. Les tests `tests/test_contrats.py` vérifient qu'un contrat cassé est bien refusé (propriétaire manquant,
   contrôle sur une colonne inconnue, niveau de service absent, DAG inconnu).

Après le déploiement, `scripts/smoke-test.sh` compare aussi les colonnes réelles de la table dans Trino avec le contrat.

## Faire évoluer un contrat

- **Ajouter une colonne** : l'ajouter au YAML (et à la transformation du DAG si elle vient de la source).
  Au chargement suivant, la table gagne la colonne par évolution de schéma Iceberg, sans réécrire les données.
- **Supprimer une colonne ou changer son type** : c'est un changement cassant pour les consommateurs.
  Le DAG refuse de charger tant que la table et le contrat divergent : il faut une nouvelle version du data product
  (nouvelle table) et une période de transition annoncée aux consommateurs listés dans `access.consumers`.
