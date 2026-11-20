"""Tests des contrats de data products et de leur traduction en contrôles SQL (sans cluster)."""

import shutil
import sys
from pathlib import Path

import pytest
import yaml

RACINE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RACINE / "scripts"))
sys.path.insert(0, str(RACINE / "dags"))

import validate_data_products as validation  # noqa: E402
from dataops.contrats import contrat, controles_sql, tous_les_contrats  # noqa: E402
from dataops.qualite import _statut  # noqa: E402


def test_les_contrats_du_depot_sont_valides():
    nombre, erreurs = validation.valider(RACINE / "data-products", RACINE / "dags")
    assert nombre >= 3
    assert erreurs == []


@pytest.fixture
def copie_contrats(tmp_path):
    shutil.copytree(RACINE / "data-products", tmp_path / "data-products")
    return tmp_path / "data-products"


def _modifier(fichier: Path, modification) -> None:
    contenu = yaml.safe_load(fichier.read_text(encoding="utf-8"))
    modification(contenu)
    fichier.write_text(yaml.safe_dump(contenu, allow_unicode=True), encoding="utf-8")


def test_un_proprietaire_manquant_est_refuse(copie_contrats):
    _modifier(copie_contrats / "reseau_fixe" / "ftth_communes.yaml", lambda c: c["metadata"].pop("owner"))
    _, erreurs = validation.valider(copie_contrats, RACINE / "dags")
    assert any("owner" in e for e in erreurs)


def test_un_controle_sur_une_colonne_inconnue_est_refuse(copie_contrats):
    def ajout(c):
        c["spec"]["quality"].append({"type": "unique", "columns": ["colonne_inexistante"]})
    _modifier(copie_contrats / "reseau_fixe" / "ftth_communes.yaml", ajout)
    _, erreurs = validation.valider(copie_contrats, RACINE / "dags")
    assert any("colonne_inexistante" in e for e in erreurs)


def test_un_niveau_de_service_absent_est_refuse(copie_contrats):
    _modifier(copie_contrats / "reseau_fixe" / "ftth_departements.yaml", lambda c: c["spec"].pop("sla"))
    _, erreurs = validation.valider(copie_contrats, RACINE / "dags")
    assert any("sla" in e for e in erreurs)


def test_un_dag_producteur_inconnu_est_refuse(copie_contrats):
    _modifier(copie_contrats / "reseau_fixe" / "ftth_communes.yaml",
              lambda c: c["spec"].__setitem__("producedBy", "dag_fantome"))
    _, erreurs = validation.valider(copie_contrats, RACINE / "dags")
    assert any("dag_fantome" in e for e in erreurs)


def test_les_controles_sql_couvrent_le_contrat():
    c = contrat("reseau_fixe.ftth_communes")
    controles = {ctl["nom"]: ctl for ctl in controles_sql(c, "trimestre = '2026-T2'")}
    assert "colonnes_obligatoires_renseignees" in controles
    assert "unicite_id_zone" in controles
    assert "volume_minimal" in controles
    assert all("trimestre = '2026-T2'" in ctl["sql"] for ctl in controles.values())
    assert controles["raccordables_coherents"]["gravite"] == "warn"
    assert controles["raccordables_coherents"]["ratio_max"] == 0.01


def test_tous_les_contrats_sont_charges():
    assert {"reseau_fixe.ftth_communes", "reseau_fixe.ftth_departements",
            "plateforme.controles_qualite"} <= set(tous_les_contrats())


@pytest.mark.parametrize(
    "echec, total, gravite, ratio, attendu",
    [
        (0, 100, "error", 0.0, "OK"),
        (1, 100, "error", 0.0, "ECHEC"),
        (1, 1000, "error", 0.01, "OK"),  # sous la tolérance
        (50, 1000, "warn", 0.01, "AVERTISSEMENT"),
    ],
)
def test_statut_d_un_controle(echec, total, gravite, ratio, attendu):
    assert _statut(echec, total, gravite, ratio) == attendu
