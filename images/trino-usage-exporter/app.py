"""Collecteur d'usage Trino.

Trino envoie un événement JSON à chaque requête terminée (event listener HTTP).
Ce service les transforme en métriques Prometheus, par utilisateur et par catalogue :
nombre de requêtes, durée, temps CPU, octets lus et requêtes lentes.
Il interroge aussi /v1/info pour savoir si Trino répond et dans quelle version.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

PORT = int(os.getenv("PORT", "8000"))
TRINO_URL = os.getenv("TRINO_URL", "http://trino.trino.svc.cluster.local:8080")
SLOW_QUERY_SECONDS = float(os.getenv("SLOW_QUERY_SECONDS", "10"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "15"))
LOG_EVENTS = os.getenv("LOG_EVENTS", "false").lower() == "true"

log = logging.getLogger("trino-usage-exporter")

LABELS = ["user", "catalog"]
QUERIES = Counter("trino_queries", "Requêtes Trino terminées", ["user", "catalog", "source", "state"])
WALL = Histogram(
    "trino_query_wall_seconds",
    "Durée des requêtes Trino (secondes)",
    LABELS,
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)
CPU = Counter("trino_query_cpu_seconds", "Temps CPU consommé par les requêtes (secondes)", LABELS)
INPUT_BYTES = Counter("trino_query_input_bytes", "Octets lus par les requêtes", LABELS)
SLOW = Counter("trino_slow_queries", "Requêtes plus longues que le seuil SLOW_QUERY_SECONDS", LABELS)
LAST_SLOW = Gauge("trino_last_slow_query_timestamp_seconds", "Horodatage de la dernière requête lente", LABELS)
REJECTED = Counter("trino_usage_events_rejected", "Événements reçus mais illisibles")
SLOW_THRESHOLD = Gauge("trino_slow_query_threshold_seconds", "Seuil des requêtes lentes (secondes)")
UP = Gauge("trino_up", "1 si le coordinateur Trino répond et a fini de démarrer")
BUILD_INFO = Gauge("trino_build_info", "Version de Trino en service", ["version"])

_ISO_DURATION = re.compile(r"^P(?:(?P<d>[\d.]+)D)?(?:T(?:(?P<h>[\d.]+)H)?(?:(?P<m>[\d.]+)M)?(?:(?P<s>[\d.]+)S)?)?$")
_UNIT_DURATION = re.compile(r"^(?P<v>[\d.]+)\s*(?P<u>ns|us|ms|s|m|h|d)$")
_UNIT_SECONDS = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def to_seconds(value) -> float:
    """Convertit une durée Trino en secondes (nombre, ISO-8601 « PT1.5S » ou « 1.50s »)."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    iso = _ISO_DURATION.match(text)
    if iso and any(iso.groupdict().values()):
        parts = {k: float(v) for k, v in iso.groupdict().items() if v}
        return parts.get("d", 0) * 86400 + parts.get("h", 0) * 3600 + parts.get("m", 0) * 60 + parts.get("s", 0)
    unit = _UNIT_DURATION.match(text)
    if unit:
        return float(unit.group("v")) * _UNIT_SECONDS[unit.group("u")]
    return float(text)


def catalogs_of(event: dict) -> str:
    """Catalogues réellement lus ou écrits ; à défaut, le catalogue de la session."""
    io = event.get("ioMetadata") or {}
    catalogs = {i.get("catalogName") for i in io.get("inputs") or [] if i.get("catalogName")}
    output = io.get("output") or {}
    if output.get("catalogName"):
        catalogs.add(output["catalogName"])
    if not catalogs:
        session_catalog = (event.get("context") or {}).get("catalog")
        if session_catalog:
            catalogs.add(session_catalog)
    return ",".join(sorted(catalogs)) or "aucun"


def handle_event(event: dict) -> dict:
    """Met à jour les métriques à partir d'un événement « requête terminée »."""
    context = event.get("context") or {}
    metadata = event.get("metadata") or {}
    stats = event.get("statistics") or {}

    user = context.get("user") or "inconnu"
    catalog = catalogs_of(event)
    source = context.get("source") or "inconnue"
    state = metadata.get("queryState") or ("FAILED" if event.get("failureInfo") else "FINISHED")
    wall = to_seconds(stats.get("wallTime"))
    cpu = to_seconds(stats.get("cpuTime"))
    input_bytes = float(stats.get("physicalInputBytes") or stats.get("totalBytes") or 0)

    QUERIES.labels(user, catalog, source, state).inc()
    WALL.labels(user, catalog).observe(wall)
    CPU.labels(user, catalog).inc(cpu)
    INPUT_BYTES.labels(user, catalog).inc(input_bytes)
    if wall >= SLOW_QUERY_SECONDS:
        SLOW.labels(user, catalog).inc()
        LAST_SLOW.labels(user, catalog).set(time.time())
        log.warning("requête lente : id=%s utilisateur=%s catalogue=%s durée=%.1fs",
                    metadata.get("queryId"), user, catalog, wall)
    return {"user": user, "catalog": catalog, "source": source, "state": state, "wall": wall, "cpu": cpu}


def poll_trino_forever() -> None:
    """Interroge /v1/info : Trino répond-il, a-t-il fini de démarrer, dans quelle version ?"""
    known_versions: set[str] = set()
    while True:
        try:
            with urllib.request.urlopen(f"{TRINO_URL}/v1/info", timeout=5) as resp:
                info = json.load(resp)
            version = str((info.get("nodeVersion") or {}).get("version", "inconnue"))
            for old in known_versions - {version}:
                BUILD_INFO.remove(old)
            known_versions = {version}
            BUILD_INFO.labels(version).set(1)
            UP.set(0 if info.get("starting") else 1)
        except Exception as exc:  # Trino arrêté, en redémarrage ou injoignable
            UP.set(0)
            log.debug("Trino injoignable : %s", exc)
        time.sleep(POLL_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code: int, body: bytes = b"", content_type: str = "text/plain; charset=utf-8") -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (nom imposé par http.server)
        if self.path == "/metrics":
            self._reply(200, generate_latest(), CONTENT_TYPE_LATEST)
        elif self.path == "/healthz":
            self._reply(200, b"ok")
        else:
            self._reply(404)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/events":
            self._reply(404)
            return
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            payload = json.loads(raw or b"{}")
            events = payload if isinstance(payload, list) else [payload]
            for event in events:
                if LOG_EVENTS:
                    log.info("événement brut : %s", json.dumps(event, ensure_ascii=False))
                handle_event(event)
            self._reply(200, b"ok")
        except Exception as exc:
            REJECTED.inc()
            log.error("événement illisible : %s", exc)
            self._reply(400, b"evenement illisible")

    def log_message(self, fmt: str, *args) -> None:  # journal HTTP silencieux
        return


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    SLOW_THRESHOLD.set(SLOW_QUERY_SECONDS)
    threading.Thread(target=poll_trino_forever, daemon=True).start()
    log.info("collecteur prêt sur le port %s (seuil requête lente : %ss)", PORT, SLOW_QUERY_SECONDS)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
