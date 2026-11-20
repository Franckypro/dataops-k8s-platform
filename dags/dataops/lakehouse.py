"""Accès au lakehouse depuis les tâches Airflow : catalogue Iceberg, MinIO (S3) et Trino."""

from __future__ import annotations

import logging
import os

from dataops.contrats import colonnes, contrat, identifiant_iceberg, partitionnement_iceberg, schema_iceberg

log = logging.getLogger(__name__)


def catalogue_iceberg():
    """Catalogue REST Iceberg ; les fichiers sont lus et écrits directement dans MinIO."""
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": os.environ["ICEBERG_REST_URI"],
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": "us-east-1",
        },
    )


def stockage_s3():
    """Système de fichiers S3 (MinIO) de PyArrow, pour déposer les fichiers bruts."""
    from pyarrow import fs

    schema, hote = os.environ["S3_ENDPOINT"].split("://", 1)
    return fs.S3FileSystem(
        endpoint_override=hote,
        scheme=schema,
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region="us-east-1",
    )


def trino():
    from airflow.providers.trino.hooks.trino import TrinoHook

    return TrinoHook(trino_conn_id="trino_default")


def requete(sql: str) -> list[tuple]:
    """Exécute une requête Trino et lit tout son résultat, y compris pour INSERT et DELETE.

    Sans lecture du résultat, le client Trino annule la requête en fermant le curseur : Trino la
    marque alors USER_CANCELED, parfois juste après le commit. Lire le résultat garantit qu'on
    attend la fin réelle de la requête.
    """
    return trino().get_records(sql)


def table_du_contrat(cle: str):
    """Crée la table si besoin, puis aligne son schéma sur le contrat.

    Ajouter une colonne au contrat l'ajoute à la table (évolution de schéma Iceberg, sans réécriture).
    Supprimer ou changer le type d'une colonne est un changement cassant : la tâche échoue.
    """
    c = contrat(cle)
    namespace, nom = identifiant_iceberg(c)
    catalogue = catalogue_iceberg()
    catalogue.create_namespace_if_not_exists(namespace)
    schema = schema_iceberg(c)
    table = catalogue.create_table_if_not_exists(
        identifier=(namespace, nom),
        schema=schema,
        partition_spec=partitionnement_iceberg(c, schema),
        properties={
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
            "comment": c["metadata"]["title"],
            "data-product.owner": c["metadata"]["owner"]["team"],
        },
    )

    attendues = {col["name"]: schema.find_field(col["name"]).field_type for col in colonnes(c)}
    presentes = {champ.name: champ.field_type for champ in table.schema().fields}
    manquantes = [n for n in attendues if n not in presentes]
    if manquantes:
        log.info("évolution de schéma de %s : ajout de %s", cle, manquantes)
        with table.update_schema() as maj:
            maj.union_by_name(schema)
        table = catalogue.load_table((namespace, nom))
        presentes = {champ.name: champ.field_type for champ in table.schema().fields}

    en_trop = sorted(set(presentes) - set(attendues))
    types_differents = sorted(n for n in attendues if presentes[n] != attendues[n])
    if en_trop or types_differents:
        raise ValueError(
            f"La table {c['spec']['table']} ne respecte plus son contrat : colonnes absentes du contrat "
            f"{en_trop}, types différents {types_differents}"
        )
    return table
