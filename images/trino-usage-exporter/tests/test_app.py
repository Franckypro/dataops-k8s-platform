"""Tests du collecteur d'usage Trino (sans cluster : on rejoue un événement réel)."""

import json
import pathlib
import sys
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import pytest
from prometheus_client import REGISTRY

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import app  # noqa: E402

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "query_completed_event.json"


def real_event() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def metric(name: str, labels: dict) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.mark.parametrize(
    "value, expected",
    [(0.25, 0.25), (3, 3.0), ("PT1M30.5S", 90.5), ("1.50s", 1.5), ("250.00ms", 0.25), (None, 0.0)],
)
def test_conversion_des_durees(value, expected):
    assert app.to_seconds(value) == pytest.approx(expected)


def test_catalogues_lus_par_une_requete_reelle():
    assert app.catalogs_of(real_event()) == "iceberg,tpch"


def test_catalogue_de_session_puis_aucun():
    assert app.catalogs_of({"context": {"catalog": "iceberg"}}) == "iceberg"
    assert app.catalogs_of({}) == "aucun"


def test_un_evenement_reel_met_a_jour_les_compteurs():
    labels = {"user": "trino", "catalog": "iceberg,tpch"}
    query_labels = {**labels, "source": "trino-cli", "state": "FINISHED"}
    queries_before = metric("trino_queries_total", query_labels)
    cpu_before = metric("trino_query_cpu_seconds_total", labels)
    bytes_before = metric("trino_query_input_bytes_total", labels)

    result = app.handle_event(real_event())

    assert result["state"] == "FINISHED"
    assert metric("trino_queries_total", query_labels) == queries_before + 1
    assert metric("trino_query_cpu_seconds_total", labels) == pytest.approx(cpu_before + 0.098)
    assert metric("trino_query_input_bytes_total", labels) == bytes_before + 1827


def test_une_requete_lente_est_comptee(monkeypatch):
    monkeypatch.setattr(app, "SLOW_QUERY_SECONDS", 10.0)
    labels = {"user": "analyste", "catalog": "iceberg"}
    before = metric("trino_slow_queries_total", labels)
    event = {
        "context": {"user": "analyste", "catalog": "iceberg"},
        "metadata": {"queryId": "q-lente", "queryState": "FINISHED"},
        "statistics": {"wallTime": 42.0, "cpuTime": 30.0},
    }
    app.handle_event(event)
    assert metric("trino_slow_queries_total", labels) == before + 1
    assert metric("trino_last_slow_query_timestamp_seconds", labels) > 0


def test_une_requete_rapide_n_est_pas_lente(monkeypatch):
    monkeypatch.setattr(app, "SLOW_QUERY_SECONDS", 10.0)
    labels = {"user": "rapide", "catalog": "tpch"}
    app.handle_event({"context": {"user": "rapide", "catalog": "tpch"}, "statistics": {"wallTime": 0.5}})
    assert metric("trino_slow_queries_total", labels) == 0


def test_une_requete_en_echec_garde_son_statut():
    event = {"context": {"user": "u"}, "metadata": {"queryState": "FAILED"}, "statistics": {}}
    assert app.handle_event(event)["state"] == "FAILED"


def test_le_serveur_http_accepte_un_evenement_et_expose_les_metriques():
    server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        request = urllib.request.Request(
            f"{base}/v1/events",
            data=FIXTURE.read_bytes(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request) as resp:
            assert resp.status == 200
        with urllib.request.urlopen(f"{base}/metrics") as resp:
            assert b"trino_queries_total" in resp.read()
        with pytest.raises(urllib.error.HTTPError) as err:
            urllib.request.urlopen(urllib.request.Request(f"{base}/v1/events", data=b"{pas du json", method="POST"))
        assert err.value.code == 400
    finally:
        server.shutdown()
