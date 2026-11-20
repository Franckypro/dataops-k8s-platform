"""Tests exécutés dans l'image Airflow (voir la CI) : les DAG se chargent et respectent les conventions."""

import os

import pytest

pytest.importorskip("airflow", reason="tests exécutés dans l'image Airflow (job « dags » de la CI)")

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # emplacement historique
    from airflow.models.dagbag import DagBag

DOSSIER_DAGS = os.getenv("AIRFLOW_DAGS", "/opt/airflow/dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DOSSIER_DAGS, include_examples=False)


def test_aucune_erreur_d_import(dagbag):
    assert dagbag.import_errors == {}


def test_les_dag_attendus_sont_presents(dagbag):
    assert {"ingestion_ftth", "controle_sla_data_products"} <= set(dagbag.dag_ids)


@pytest.mark.parametrize("dag_id", ["ingestion_ftth", "controle_sla_data_products"])
def test_conventions(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag.tags, "chaque DAG porte des tags"
    assert dag.catchup is False, "pas de rattrapage automatique des exécutions passées"
    assert dag.max_active_runs == 1, "une seule exécution à la fois"
    assert dag.default_args.get("owner"), "chaque DAG a un propriétaire"
    assert dag.doc_md, "chaque DAG est documenté"


def test_chaine_de_l_ingestion(dagbag):
    dag = dagbag.get_dag("ingestion_ftth")
    assert dag.get_task("charger_communes").upstream_task_ids == {"telecharger_source"}
    assert dag.get_task("publier_departements").upstream_task_ids == {"charger_communes", "controler_communes"}
    assert dag.get_task("controler_departements").upstream_task_ids == {"charger_communes", "publier_departements"}
    assert "simuler_echec" in dag.params
