#!/usr/bin/env python3
"""Valide les contrats de data products (data-products/<domaine>/<nom>.yaml). Lancé par la CI.

1. Chaque contrat respecte le schéma JSON data-products/_schema/data-product.schema.json.
2. Règles de cohérence que le schéma ne peut pas exprimer : emplacement du fichier, nom de table,
   colonnes uniques, partitions et contrôles portant sur des colonnes existantes, DAG producteur
   présent dans dags/, data products consommateurs existants.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

RACINE = Path(__file__).resolve().parents[1]
REFERENCE_DATA_PRODUCT = re.compile(r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")


def charger_validateur(dossier: Path) -> Draft202012Validator:
    schema = json.loads((dossier / "_schema" / "data-product.schema.json").read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def dag_ids(dossier_dags: Path) -> set[str]:
    ids = set()
    for fichier in dossier_dags.glob("*.py"):
        ids.update(re.findall(r'dag_id="([a-z0-9_]+)"', fichier.read_text(encoding="utf-8")))
    return ids


def valider(dossier: Path, dossier_dags: Path) -> tuple[int, list[str]]:
    validateur = charger_validateur(dossier)
    dags = dag_ids(dossier_dags)
    fichiers = sorted(dossier.glob("*/*.yaml"))
    erreurs: list[str] = []
    contrats: dict[str, tuple[str, dict]] = {}

    for fichier in fichiers:
        rel = fichier.relative_to(dossier.parent).as_posix()
        contrat = yaml.safe_load(fichier.read_text(encoding="utf-8"))
        problemes = sorted(validateur.iter_errors(contrat), key=lambda e: list(e.path))
        for p in problemes:
            chemin = "/".join(str(x) for x in p.path) or "(racine)"
            erreurs.append(f"{rel} : {chemin} : {p.message}")
        if problemes:
            continue

        meta, spec = contrat["metadata"], contrat["spec"]
        cle = f"{meta['domain']}.{meta['name']}"
        if (fichier.parent.name, fichier.stem) != (meta["domain"], meta["name"]):
            erreurs.append(f"{rel} : le fichier doit être data-products/{meta['domain']}/{meta['name']}.yaml")
        if spec["table"] != f"iceberg.{cle}":
            erreurs.append(f"{rel} : spec.table doit valoir iceberg.{cle}")
        if cle in contrats:
            erreurs.append(f"{rel} : le data product {cle} est déjà défini dans {contrats[cle][0]}")
        contrats[cle] = (rel, contrat)

        colonnes = [col["name"] for col in spec["schema"]]
        doublons = sorted({c for c in colonnes if colonnes.count(c) > 1})
        if doublons:
            erreurs.append(f"{rel} : colonnes en double : {doublons}")
        for partition in spec.get("partitioning", []):
            if partition not in colonnes:
                erreurs.append(f"{rel} : partition sur une colonne inconnue : {partition}")
        for controle in spec["quality"]:
            cibles = list(controle.get("columns", [])) + ([controle["column"]] if "column" in controle else [])
            for colonne in cibles:
                if colonne not in colonnes:
                    erreurs.append(f"{rel} : contrôle {controle['type']} sur une colonne inconnue : {colonne}")
        if spec.get("producedBy") and spec["producedBy"] not in dags:
            erreurs.append(f"{rel} : producedBy référence un DAG absent de dags/ : {spec['producedBy']}")

    for rel, contrat in contrats.values():
        for consommateur in contrat["spec"]["access"].get("consumers", []):
            if REFERENCE_DATA_PRODUCT.match(consommateur) and consommateur not in contrats:
                erreurs.append(f"{rel} : consommateur {consommateur} inconnu (aucun contrat ne le définit)")

    return len(fichiers), erreurs


def main() -> int:
    nombre, erreurs = valider(RACINE / "data-products", RACINE / "dags")
    for erreur in erreurs:
        print(f"ERREUR {erreur}")
    if nombre == 0:
        print("ERREUR aucun contrat trouvé dans data-products/")
        return 1
    print(f"{nombre} contrat(s) vérifié(s), {len(erreurs)} erreur(s)")
    return 1 if erreurs else 0


if __name__ == "__main__":
    sys.exit(main())
