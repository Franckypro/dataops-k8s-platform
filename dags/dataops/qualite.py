"""Exécution des contrôles qualité déclarés dans les contrats, et historisation des résultats."""

from __future__ import annotations

import logging

from dataops.contrats import contrat, controles_sql
from dataops.lakehouse import requete, table_du_contrat

try:  # échec définitif, sans nouvelle tentative : relancer ne corrigera pas les données
    from airflow.exceptions import AirflowFailException as _EchecDefinitif
except ImportError:  # tests unitaires sans Airflow
    _EchecDefinitif = Exception

log = logging.getLogger(__name__)

TABLE_RESULTATS = "plateforme.controles_qualite"


class ControleQualiteEnEchec(_EchecDefinitif):
    """Au moins un contrôle bloquant a échoué : les données ne doivent pas être publiées."""


def _statut(echec: int, total: int, gravite: str, ratio_max: float) -> str:
    if echec == 0 or (total and echec / total <= ratio_max):
        return "OK"
    return "AVERTISSEMENT" if gravite == "warn" else "ECHEC"


def _texte(valeur: str) -> str:
    return "'" + valeur.replace("'", "''") + "'"


def executer_controles(cle: str, filtre: str, run_id: str, controles_en_plus: list[dict] | None = None) -> list[dict]:
    """Exécute les contrôles du contrat `cle` sur la partition `filtre`, puis les historise dans Iceberg."""
    resultats = []
    for ctl in controles_sql(contrat(cle), filtre) + (controles_en_plus or []):
        echec, total = requete(ctl["sql"])[0]
        echec, total = int(echec or 0), int(total or 0)
        statut = _statut(echec, total, ctl["gravite"], ctl["ratio_max"])
        resultats.append({**ctl, "echec": echec, "total": total, "statut": statut})
        log.info("%-40s %-13s %8d en échec sur %8d", ctl["nom"], statut, echec, total)

    table_du_contrat(TABLE_RESULTATS)
    lignes = ",\n".join(
        f"(current_timestamp, current_date, {_texte(cle)}, {_texte(filtre)}, {_texte(r['nom'])}, "
        f"{_texte(r['gravite'])}, {r['total']}, {r['echec']}, {_texte(r['statut'])}, {_texte(run_id)})"
        for r in resultats
    )
    requete(
        f"INSERT INTO iceberg.{TABLE_RESULTATS} (date_controle, jour, data_product, partition_controlee, controle, "
        f"gravite, lignes_controlees, lignes_en_echec, statut, dag_run_id) VALUES {lignes}"
    )

    bloquants = [r["nom"] for r in resultats if r["statut"] == "ECHEC"]
    if bloquants:
        raise ControleQualiteEnEchec(f"{cle} ({filtre}) : contrôles bloquants en échec : {', '.join(bloquants)}")
    return resultats
