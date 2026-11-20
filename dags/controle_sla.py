"""
### Niveau de service des data products

Toutes les heures, vérifie pour chaque contrat de `data-products/` que la table existe et que son
dernier commit Iceberg date de moins de `sla.freshness_max_hours`. Le DAG échoue (et l'alerte
« DAG en échec » se déclenche) dès qu'un data product est absent ou trop ancien.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="controle_sla_data_products",
    description="Fraîcheur des data products comparée à leur contrat (sla.freshness_max_hours)",
    schedule="15 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    # Activé par scripts/up.sh après le premier chargement, pour ne pas alerter sur une plateforme vide
    is_paused_upon_creation=True,
    default_args={"owner": "equipe-plateforme", "retries": 0},
    tags=["data-product", "sla"],
    doc_md=__doc__,
)
def controle_sla_data_products():
    @task
    def verifier_fraicheur() -> list[dict]:
        from airflow.exceptions import AirflowFailException

        from dataops.contrats import identifiant_iceberg, tous_les_contrats
        from dataops.lakehouse import requete

        bilan, hors_sla = [], []
        for cle, c in tous_les_contrats().items():
            namespace, table = identifiant_iceberg(c)
            seuil = float(c["spec"]["sla"]["freshness_max_hours"])
            try:
                age = requete(
                    f'SELECT date_diff(\'second\', max(committed_at), current_timestamp) / 3600.0 '
                    f'FROM iceberg.{namespace}."{table}$snapshots"'
                )[0][0]
            except Exception as exc:  # table absente du catalogue
                log.error("%s : table introuvable (%s)", cle, str(exc).splitlines()[0])
                age = None
            ok = age is not None and age <= seuil
            bilan.append({"data_product": cle, "age_heures": None if age is None else round(float(age), 2),
                          "seuil_heures": seuil, "respecte": ok})
            log.info("%-35s âge %-8s seuil %5.1f h  %s", cle, "absent" if age is None else f"{float(age):.1f} h",
                     seuil, "OK" if ok else "HORS SLA")
            if not ok:
                hors_sla.append(cle)
        if hors_sla:
            raise AirflowFailException(f"data products hors niveau de service : {', '.join(hors_sla)}")
        return bilan

    verifier_fraicheur()


controle_sla_data_products()
