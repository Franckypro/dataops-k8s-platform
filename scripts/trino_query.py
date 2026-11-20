#!/usr/bin/env python3
"""Client minimal de l'API HTTP de Trino : exécute une requête et affiche le résultat en TSV.

Utilisé par les scripts à la place du client « trino » lancé dans le pod : ce client Java partage la limite mémoire
du conteneur de Trino et peut être tué faute de mémoire (code 137) pendant une requête lourde.

    python scripts/trino_query.py "SELECT version()"
    python scripts/trino_query.py --user analyste "SELECT count(*) FROM tpch.sf1.orders"
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request


def executer(sql: str, utilisateur: str, url: str) -> int:
    entetes = {"X-Trino-User": utilisateur, "X-Trino-Source": "scripts-plateforme"}
    requete = urllib.request.Request(f"{url}/v1/statement", data=sql.encode("utf-8"), headers=entetes, method="POST")
    with urllib.request.urlopen(requete, timeout=60) as reponse:
        page = json.load(reponse)
    while True:
        for ligne in page.get("data") or []:
            print("\t".join("" if valeur is None else str(valeur) for valeur in ligne))
        if page.get("error"):
            print(f"Query {page.get('id')} failed: {page['error'].get('message')}", file=sys.stderr)
            return 1
        suivante = page.get("nextUri")
        if not suivante:
            return 0
        with urllib.request.urlopen(urllib.request.Request(suivante, headers=entetes), timeout=600) as reponse:
            page = json.load(reponse)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sql")
    parser.add_argument("--user", default="plateforme")
    parser.add_argument("--url", default="http://localhost:30088")
    args = parser.parse_args()
    return executer(args.sql, args.user, args.url)


if __name__ == "__main__":
    sys.exit(main())
