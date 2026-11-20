"""Tests du client HTTP de Trino (scripts/trino_query.py) contre un faux serveur local."""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import trino_query  # noqa: E402


class FauxTrino(BaseHTTPRequestHandler):
    """Répond 503 aux premiers envois (Trino qui démarre), puis renvoie le résultat en deux pages."""

    refus_restants = 0
    requetes_recues = 0

    def _json(self, corps: dict) -> None:
        brut = json.dumps(corps).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(brut)))
        self.end_headers()
        self.wfile.write(brut)

    def do_POST(self):  # noqa: N802
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if FauxTrino.refus_restants > 0:
            FauxTrino.refus_restants -= 1
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        FauxTrino.requetes_recues += 1
        hote = f"http://127.0.0.1:{self.server.server_address[1]}"
        self._json({"id": "q1", "nextUri": f"{hote}/v1/statement/q1/1"})

    def do_GET(self):  # noqa: N802
        self._json({"id": "q1", "data": [["483", None]]})

    def log_message(self, *args):
        return


@pytest.fixture
def faux_trino():
    serveur = ThreadingHTTPServer(("127.0.0.1", 0), FauxTrino)
    threading.Thread(target=serveur.serve_forever, daemon=True).start()
    FauxTrino.refus_restants = 0
    FauxTrino.requetes_recues = 0
    yield f"http://127.0.0.1:{serveur.server_address[1]}"
    serveur.shutdown()


def test_resultat_affiche_en_tsv(faux_trino, capsys):
    assert trino_query.executer("SELECT version()", "plateforme", faux_trino, pause=0) == 0
    assert capsys.readouterr().out == "483\t\n"


def test_reessaie_tant_que_trino_demarre(faux_trino, capsys):
    FauxTrino.refus_restants = 2
    assert trino_query.executer("SELECT version()", "plateforme", faux_trino, pause=0) == 0
    sortie = capsys.readouterr()
    assert sortie.out == "483\t\n"
    assert sortie.err.count("nouvel essai") == 2
    assert FauxTrino.requetes_recues == 1


def test_abandonne_apres_le_nombre_d_essais():
    # Rien n'écoute sur ce port : connexion refusée à chaque essai
    requete = trino_query.urllib.request.Request("http://127.0.0.1:9/v1/statement", data=b"SELECT 1", method="POST")
    with pytest.raises(OSError):
        trino_query.lire_json(requete, delai=2, tentatives=2, pause=0)
