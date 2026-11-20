# Supervision, alertes, usages et coûts

## Ce que collecte Prometheus

| Cible | Métriques | Déclarée dans |
|---|---|---|
| Collecteur d'usage Trino | requêtes, durée, temps CPU, octets lus, requêtes lentes (par utilisateur et par catalogue), disponibilité et version de Trino | `charts/supervision/templates/servicemonitors.yaml` |
| Airflow (StatsD → Prometheus) | exécutions de DAG réussies ou en échec, statut final de chaque tâche, battement de cœur du scheduler | idem |
| MinIO | capacité, volume par bucket | idem |
| OpenCost | coût horaire des nœuds, ressources allouées par conteneur | `values/opencost.yaml` |
| Kubernetes (kubelet, kube-state-metrics, node-exporter) | CPU et mémoire par pod et par namespace | `values/kube-prometheus-stack.yaml` |

## Le collecteur d'usage Trino

Trino envoie un événement JSON à chaque requête terminée (event listener HTTP, configuré dans `values/trino.yaml`).
Le collecteur (`images/trino-usage-exporter/app.py`, une centaine de lignes de Python) en tire :

- `trino_queries_total{user, catalog, source, state}` : requêtes terminées ;
- `trino_query_cpu_seconds_total{user, catalog}` et `trino_query_input_bytes_total{user, catalog}` : ce que chaque
  utilisateur consomme, par catalogue (la base d'une refacturation interne) ;
- `trino_query_wall_seconds{user, catalog}` : histogramme des durées (95e centile dans Grafana) ;
- `trino_slow_queries_total` et `trino_last_slow_query_timestamp_seconds` : requêtes au-delà du seuil (10 s) ;
- `trino_up` et `trino_build_info{version}` : disponibilité et version, lues sur `/v1/info`.

Le catalogue d'une requête est celui des tables réellement lues ou écrites (`iceberg`, `tpch`, ou `iceberg,tpch`
pour une jointure entre les deux), à défaut celui de la session.

Ses tests (`images/trino-usage-exporter/tests`) rejouent un événement réel capturé sur Trino 483.

## Les alertes

| Alerte | Se déclenche quand | Gravité |
|---|---|---|
| `AirflowDagEnEchec` | une exécution de DAG échoue (fenêtre de 15 min) | warning |
| `AirflowTacheEnEchec` | une tâche échoue (fenêtre de 15 min) | warning |
| `AirflowSchedulerArrete` | le scheduler ne bat plus depuis 5 min | critical |
| `TrinoIndisponible` | Trino ne répond plus depuis 2 min | critical |
| `TrinoRequetesLentes` | une requête a dépassé le seuil dans les 15 dernières minutes | warning |
| `TrinoTauxEchecEleve` | plus de 20 % de requêtes en échec sur 15 min | warning |
| `MinioIndisponible` | MinIO ne répond plus depuis 2 min | critical |

Détail utile : `increase()` ne voit pas la toute première valeur d'un compteur qui apparaît. Le premier échec d'un DAG
créerait donc la série sans déclencher l'alerte. Les règles Airflow ajoutent
`... unless ... offset 15m` pour couvrir ce cas, et l'alerte de requête lente s'appuie sur l'horodatage de la dernière
requête lente plutôt que sur un compteur.

Pour les voir se déclencher :

```bash
EXPECT_STATE=failed ./scripts/run-dag.sh ingestion_ftth '{"simuler_echec": true}'   # DAG en échec
./scripts/test-alertes.sh                                                        # ce que fait la CI
```

Les alertes sont visibles dans Prometheus (<http://localhost:30090/alerts>), Alertmanager
(<http://localhost:30093>) et sur le tableau Grafana « Plateforme DataOps ». En production, Alertmanager les
enverrait vers Teams, Slack, un e-mail ou un outil d'astreinte.

## Les tableaux Grafana (dossier « DataOps »)

- **Plateforme DataOps : vue d'ensemble** : état et version de Trino, exécutions de DAG réussies et en échec,
  alertes en cours, volume stocké, CPU et mémoire par namespace.
- **Usages Trino par utilisateur et par catalogue** : requêtes, temps CPU, octets lus, 95e centile de durée,
  requêtes lentes.
- **FinOps : coût par namespace** : coût horaire du cluster, projection mensuelle, coût par namespace, ressources
  allouées comparées aux ressources consommées.

Ils sont versionnés en JSON dans `charts/supervision/dashboards/` et chargés automatiquement par Grafana.

## Les coûts avec OpenCost

OpenCost croise les ressources réservées et consommées par chaque conteneur avec un prix par vCPU, par Gio de mémoire
et par Gio de stockage. Un cluster local n'a pas de facture : les prix sont fixés dans `values/opencost.yaml`
(0,04 $ par vCPU et par heure, 0,005 $ par Gio et par heure ; le dollar est la devise par défaut d'OpenCost).
**Ce sont des tarifs indicatifs**, qui servent à montrer la
ventilation par namespace ; on les remplacerait par le coût réel de l'infrastructure (ou par la facture du
fournisseur cloud, qu'OpenCost sait lire).

- Interface OpenCost : <http://localhost:30091> (allocation par namespace, pod, conteneur).
- API : `kubectl get --raw "/api/v1/namespaces/opencost/services/opencost:9003/proxy/allocation/compute?window=1d&aggregate=namespace"`.

Les écarts entre ressources allouées et consommées (tableau FinOps) montrent où les réservations sont trop larges :
c'est le premier levier d'optimisation des coûts sur Kubernetes.
