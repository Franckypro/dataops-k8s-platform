"""
### Ingestion : déploiement de la fibre et fermeture du cuivre (ANCT)

1. **telecharger_source** : télécharge le CSV publié sur data.gouv.fr et le dépose tel quel dans MinIO
   (`s3://landing/ftth/...`), avec son empreinte SHA-256.
2. **charger_communes** : lit le fichier brut, applique le contrat `reseau_fixe.ftth_communes`
   (noms, types, taux calculé) et remplace la partition du trimestre dans la table Iceberg.
3. **controler_communes** : exécute les contrôles qualité du contrat avec Trino et les historise.
4. **publier_departements** : Trino calcule l'agrégat par département (`reseau_fixe.ftth_departements`).
5. **controler_departements** : contrôles qualité de l'agrégat.

Relancer le DAG ne crée pas de doublon : chaque trimestre est une partition remplacée en entier.
Le paramètre `simuler_echec` ajoute un contrôle bloquant qui échoue, pour tester l'alerte « DAG en échec ».

Source : ANCT, Indicateur France Très Haut Débit, Licence Ouverte 2.0.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task

log = logging.getLogger(__name__)

URL_SOURCE = os.getenv(
    "FTTH_SOURCE_URL", "https://www.data.gouv.fr/fr/datasets/r/35dc0362-960f-4353-8a9b-0cdcf5a1a270"
)
CONTRAT_COMMUNES = "reseau_fixe.ftth_communes"
CONTRAT_DEPARTEMENTS = "reseau_fixe.ftth_departements"


@dag(
    dag_id="ingestion_ftth",
    description="Fibre et cuivre par commune (ANCT) : source -> MinIO -> Iceberg -> contrôles -> agrégat",
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "domaine-reseau-fixe", "retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "simuler_echec": Param(False, type="boolean",
                               description="Ajoute un contrôle bloquant qui échoue (test de l'alerte)"),
    },
    tags=["data-product", "reseau_fixe", "iceberg"],
    doc_md=__doc__,
)
def ingestion_ftth():
    @task
    def telecharger_source() -> dict:
        import csv

        import requests

        from dataops.lakehouse import stockage_s3

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            empreinte = hashlib.sha256()
            with requests.get(URL_SOURCE, stream=True, timeout=120) as reponse:
                reponse.raise_for_status()
                for bloc in reponse.iter_content(chunk_size=1 << 20):
                    tmp.write(bloc)
                    empreinte.update(bloc)
            chemin_local = tmp.name

        with open(chemin_local, encoding="utf-8-sig", newline="") as f:
            lecteur = csv.DictReader(f)
            premiere = next(lecteur)
            nb_lignes = 1 + sum(1 for _ in lecteur)
        trimestre = premiere["trimestre"].strip().replace(" ", "-")  # « 2026 T2 » -> « 2026-T2 »

        horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        cle = f"landing/ftth/trimestre={trimestre}/indicateur-ftth_{horodatage}.csv"
        s3 = stockage_s3()
        with open(chemin_local, "rb") as source, s3.open_output_stream(cle) as cible:
            cible.write(source.read())
        os.remove(chemin_local)

        log.info("fichier déposé : s3://%s (%d lignes, sha256 %s)", cle, nb_lignes, empreinte.hexdigest())
        return {"cle_s3": cle, "trimestre": trimestre, "lignes": nb_lignes, "sha256": empreinte.hexdigest()}

    @task
    def charger_communes(fichier: dict) -> str:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.csv as pacsv
        from pyiceberg.expressions import EqualTo

        from dataops.contrats import FORMAT_TRIMESTRE, colonnes, contrat, schema_arrow
        from dataops.lakehouse import stockage_s3, table_du_contrat

        trimestre = fichier["trimestre"]
        if not FORMAT_TRIMESTRE.match(trimestre):
            raise ValueError(f"trimestre inattendu dans la source : {trimestre!r}")

        c = contrat(CONTRAT_COMMUNES)
        cible = schema_arrow(c)
        # Types lus dans le CSV : texte pour les codes (« 01001 », « 2A004 »), nombre pour les comptages
        types_source = {
            col["source"]: (pa.float64() if col["type"] == "double" else pa.string())
            for col in colonnes(c) if col.get("source")
        }
        with stockage_s3().open_input_stream(fichier["cle_s3"]) as flux:
            brut = pacsv.read_csv(
                flux,
                convert_options=pacsv.ConvertOptions(column_types=types_source, include_columns=list(types_source),
                                                     strings_can_be_null=True, null_values=[""]),
            )

        donnees = {col["name"]: brut[col["source"]] for col in colonnes(c) if col.get("source")}
        donnees["trimestre"] = pa.array([trimestre] * brut.num_rows, pa.string())
        locaux = donnees["locaux"]
        donnees["taux_raccordables_ftth"] = pc.if_else(
            pc.greater(locaux, 0), pc.divide(donnees["locaux_raccordables_ftth"], locaux), None
        )
        horodatage = datetime.now(timezone.utc)
        donnees["date_ingestion"] = pa.array([horodatage] * brut.num_rows, cible.field("date_ingestion").type)
        tableau = pa.table({nom: donnees[nom] for nom in cible.names}, schema=cible)

        table = table_du_contrat(CONTRAT_COMMUNES)
        table.overwrite(tableau, overwrite_filter=EqualTo("trimestre", trimestre))
        log.info("%s : %d lignes écrites dans la partition trimestre=%s (snapshot %s)",
                 c["spec"]["table"], tableau.num_rows, trimestre, table.refresh().current_snapshot().snapshot_id)
        return trimestre

    @task
    def controler_communes(trimestre: str) -> None:
        from dataops.qualite import executer_controles

        contexte = get_current_context()
        en_plus = []
        if contexte["params"].get("simuler_echec"):
            en_plus.append({"nom": "echec_simule", "sql": "SELECT 1, 1", "gravite": "error", "ratio_max": 0.0})
        executer_controles(CONTRAT_COMMUNES, f"trimestre = '{trimestre}'", contexte["run_id"], en_plus)

    @task
    def publier_departements(trimestre: str) -> None:
        from dataops.lakehouse import requete, table_du_contrat

        table_du_contrat(CONTRAT_DEPARTEMENTS)
        requete(f"DELETE FROM iceberg.reseau_fixe.ftth_departements WHERE trimestre = '{trimestre}'")
        requete(f"""
            INSERT INTO iceberg.reseau_fixe.ftth_departements
              (code_departement, departement, nb_zones, locaux, locaux_raccordables_ftth, taux_raccordables_ftth,
               fermeture_cuivre_fin_2026, fermeture_cuivre_fin_2030, trimestre, date_calcul)
            SELECT code_departement,
                   departement,
                   count(*),
                   sum(locaux),
                   sum(coalesce(locaux_raccordables_ftth, 0)),
                   sum(coalesce(locaux_raccordables_ftth, 0)) / nullif(sum(locaux), 0),
                   sum(coalesce(fermeture_cuivre_fin_2026, 0)),
                   sum(coalesce(fermeture_cuivre_fin_2030, 0)),
                   trimestre,
                   current_timestamp
            FROM iceberg.reseau_fixe.ftth_communes
            WHERE trimestre = '{trimestre}'
            GROUP BY code_departement, departement, trimestre
        """)

    @task
    def controler_departements(trimestre: str) -> None:
        from dataops.qualite import executer_controles

        executer_controles(CONTRAT_DEPARTEMENTS, f"trimestre = '{trimestre}'", get_current_context()["run_id"])

    trimestre = charger_communes(telecharger_source())
    controle = controler_communes(trimestre)
    publication = publier_departements(trimestre)
    controle >> publication >> controler_departements(trimestre)


ingestion_ftth()
