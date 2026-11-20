"""Lecture des contrats de data products (data-products/<domaine>/<nom>.yaml).

Le contrat est la source de vérité : il donne le schéma de la table Iceberg (colonnes, types,
commentaires, partitions) et les contrôles qualité que les DAG exécutent après chaque chargement.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path

import yaml

DOSSIER_CONTRATS = Path(os.getenv("DATA_PRODUCTS_DIR", Path(__file__).resolve().parents[2] / "data-products"))
FORMAT_TRIMESTRE = re.compile(r"^\d{4}-T[1-4]$")


@lru_cache(maxsize=1)
def tous_les_contrats() -> dict[str, dict]:
    """Contrats indexés par « domaine.nom » (ex. reseau_fixe.ftth_communes)."""
    contrats = {}
    for fichier in sorted(DOSSIER_CONTRATS.glob("*/*.yaml")):
        contrat = yaml.safe_load(fichier.read_text(encoding="utf-8"))
        meta = contrat["metadata"]
        contrats[f"{meta['domain']}.{meta['name']}"] = contrat
    return contrats


def contrat(cle: str) -> dict:
    return tous_les_contrats()[cle]


def colonnes(c: dict) -> list[dict]:
    return c["spec"]["schema"]


def identifiant_iceberg(c: dict) -> tuple[str, str]:
    """« iceberg.reseau_fixe.ftth_communes » -> (« reseau_fixe », « ftth_communes »)."""
    _, namespace, table = c["spec"]["table"].split(".")
    return namespace, table


def schema_iceberg(c: dict):
    """Schéma Iceberg déduit du contrat. Les descriptions deviennent les commentaires des colonnes."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import BooleanType, DateType, DoubleType, LongType, NestedField, StringType, TimestamptzType

    types = {"string": StringType(), "double": DoubleType(), "long": LongType(), "boolean": BooleanType(),
             "date": DateType(), "timestamp": TimestamptzType()}
    # Les colonnes sont optionnelles au sens Iceberg : l'obligation de remplissage est vérifiée
    # par les contrôles qualité, qui donnent un message clair au lieu d'un rejet d'écriture.
    champs = [
        NestedField(field_id=i, name=col["name"], field_type=types[col["type"]], required=False,
                    doc=col["description"])
        for i, col in enumerate(colonnes(c), start=1)
    ]
    return Schema(*champs)


def partitionnement_iceberg(c: dict, schema):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    champs = [
        PartitionField(source_id=schema.find_field(nom).field_id, field_id=1000 + i,
                       transform=IdentityTransform(), name=nom)
        for i, nom in enumerate(c["spec"].get("partitioning", []))
    ]
    return PartitionSpec(*champs)


def schema_arrow(c: dict):
    """Schéma Arrow équivalent, pour construire les données à écrire avec PyIceberg."""
    import pyarrow as pa

    types = {"string": pa.string(), "double": pa.float64(), "long": pa.int64(), "boolean": pa.bool_(),
             "date": pa.date32(), "timestamp": pa.timestamp("us", tz="UTC")}
    return pa.schema([pa.field(col["name"], types[col["type"]], nullable=True) for col in colonnes(c)])


def _litteral(valeur) -> str:
    if isinstance(valeur, str):
        return "'" + valeur.replace("'", "''") + "'"
    return repr(valeur)


def controles_sql(c: dict, filtre: str | None = None) -> list[dict]:
    """Traduit les contrôles du contrat en requêtes Trino.

    Chaque requête renvoie une ligne : (lignes en échec, lignes contrôlées).
    `filtre` limite le contrôle à la partition qui vient d'être chargée.
    """
    table = c["spec"]["table"]
    where = f" WHERE {filtre}" if filtre else ""
    controles = []

    def ajouter(nom: str, sql: str, gravite: str = "error", ratio_max: float = 0.0) -> None:
        controles.append({"nom": nom, "sql": sql, "gravite": gravite, "ratio_max": ratio_max})

    obligatoires = [col["name"] for col in colonnes(c) if col.get("required")]
    if obligatoires:
        condition = " OR ".join(f"{nom} IS NULL" for nom in obligatoires)
        ajouter("colonnes_obligatoires_renseignees", f"SELECT count_if({condition}), count(*) FROM {table}{where}")

    for q in c["spec"]["quality"]:
        gravite = q.get("severity", "error")
        ratio = float(q.get("max_failure_ratio", 0))
        if q["type"] == "not_null":
            condition = " OR ".join(f"{nom} IS NULL" for nom in q["columns"])
            ajouter("non_vide_" + "_".join(q["columns"]),
                    f"SELECT count_if({condition}), count(*) FROM {table}{where}", gravite, ratio)
        elif q["type"] == "unique":
            cols = ", ".join(q["columns"])
            ajouter("unicite_" + "_".join(q["columns"]),
                    f"SELECT coalesce(sum(n) - count(*), 0), coalesce(sum(n), 0) "
                    f"FROM (SELECT {cols}, count(*) AS n FROM {table}{where} GROUP BY {cols})", gravite, ratio)
        elif q["type"] == "accepted_values":
            valeurs = ", ".join(_litteral(v) for v in q["values"])
            ajouter(f"valeurs_autorisees_{q['column']}",
                    f"SELECT count_if({q['column']} NOT IN ({valeurs})), count(*) FROM {table}{where}",
                    gravite, ratio)
        elif q["type"] == "range":
            bornes = []
            if "min" in q:
                bornes.append(f"{q['column']} < {q['min']}")
            if "max" in q:
                bornes.append(f"{q['column']} > {q['max']}")
            ajouter(f"bornes_{q['column']}",
                    f"SELECT count_if({' OR '.join(bornes)}), count(*) FROM {table}{where}", gravite, ratio)
        elif q["type"] == "row_count_min":
            ajouter("volume_minimal",
                    f"SELECT CASE WHEN count(*) < {int(q['min'])} THEN 1 ELSE 0 END, count(*) FROM {table}{where}",
                    gravite)
        elif q["type"] == "expression":
            ajouter(q["name"], f"SELECT count_if(NOT ({q['condition']})), count(*) FROM {table}{where}",
                    gravite, ratio)
    return controles
