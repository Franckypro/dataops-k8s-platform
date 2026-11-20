#!/usr/bin/env python3
"""Client minimal de l'API HTTP de Trino : exécute une requête et affiche le résultat en TSV.

Utilisé par les scripts à la place du client « trino » lancé dans le pod : ce client Java partage la limite mémoire
du conteneur de Trino et peut être tué faute de mémoire (code 137) pendant une requête lourde.

Les erreurs passagères (connexion refusée ou coupée, réponses 502, 503 ou 504) sont réessayées : juste après une
montée de version, le NodePort peut encore envoyer une connexion vers l'ancien pod en cours d'arrêt, et Trino répond
503 tant qu'il démarre. Réessayer est sans risque : l'envoi d'une requête et la lecture de nextUri sont rejouables.

    python scripts/trino_query.py "SELECT version()"
    python scripts/trino_query.py --user analyste "SELECT count(*) FROM tpch.sf1.orders"
"""

from __future__ import annotations

import argparse
import http.client
import json
import sys
import time
import urllib.error
import urllib.request

CODES_TEMPORAIRES = {502, 503, 504}


def lire_json(requete: urllib.request.Request, delai: float, tentatives: int = 12, pause: float = 5.0) -> dict:
    """Envoie la requête HTTP et renvoie le JSON, en réessayant sur les erreurs passagères."""
    for essai in range(1, tentatives + 1):
        try:
            with urllib.request.urlopen(requete, timeout=delai) as reponse:
                return json.load(reponse)
        except urllib.error.HTTPError as exc:
            if exc.code not in CODES_TEMPORAIRES or essai == tentatives:
                raise
            raison = f"HTTP {exc.code}"
        except (ConnectionError, TimeoutError, http.client.HTTPException, urllib.error.URLError) as exc:
            if essai == tentatives:
                raise
            raison = type(exc).__name__
        print(f"Trino indisponible ({raison}), nouvel essai dans {pause:g} s ({essai}/{tentatives})", file=sys.stderr)
        time.sleep(pause)
    raise RuntimeError("inaccessible")  # pour les analyseurs de code : la boucle renvoie ou lève toujours


def executer(sql: str, utilisateur: str, url: str, pause: float = 5.0) -> int:
    entetes = {"X-Trino-User": utilisateur, "X-Trino-Source": "scripts-plateforme"}
    envoi = urllib.request.Request(f"{url}/v1/statement", data=sql.encode("utf-8"), headers=entetes, method="POST")
    page = lire_json(envoi, delai=60, pause=pause)
    while True:
        for ligne in page.get("data") or []:
            print("\t".join("" if valeur is None else str(valeur) for valeur in ligne))
        if page.get("error"):
            print(f"Query {page.get('id')} failed: {page['error'].get('message')}", file=sys.stderr)
            return 1
        suivante = page.get("nextUri")
        if not suivante:
            return 0
        page = lire_json(urllib.request.Request(suivante, headers=entetes), delai=600, pause=pause)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sql")
    parser.add_argument("--user", default="plateforme")
    parser.add_argument("--url", default="http://localhost:30088")
    args = parser.parse_args()
    return executer(args.sql, args.user, args.url)


if __name__ == "__main__":
    sys.exit(main())
