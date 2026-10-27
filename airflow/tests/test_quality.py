"""Pruebas de una regla de calidad: caso válido y caso inválido."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from quality import evaluate_batch, evaluate_rows, rule_freshness, rule_volume


def _valid_event(uid: str = "ev1") -> dict:
    return {
        "source_id": "usgs_feed",
        "event_uid": uid,
        "mag": 4.5,
        "place": "Somewhere, Chile",
        "region": "Chile",
        "event_time": datetime.now(timezone.utc),
        "updated_time": None,
        "tsunami": False,
        "sig": 50,
        "status": "automatic",
        "mag_type": "mw",
        "event_type": "earthquake",
        "url": None,
        "longitude": -70.0,
        "latitude": -33.0,
        "depth_km": 30.0,
    }


# ---------- Caso VÁLIDO ----------

def test_valid_row_passes_all_rules():
    valid, rejected = evaluate_rows([_valid_event()])
    assert len(valid) == 1
    assert rejected == []


def test_batch_valid_reports_pass():
    rows = [_valid_event(f"ev{i}") for i in range(5)]
    report = evaluate_batch(rows, latest_ingested_at=datetime.now(timezone.utc))
    assert report["status"] == "pass"
    assert report["records_valid"] == 5
    assert report["records_rejected"] == 0
    assert report["score"] == 100.0
    assert report["rules_evaluated"] >= 6


# ---------- Caso INVÁLIDO ----------

def test_invalid_row_is_rejected_with_reason():
    bad = _valid_event()
    bad["latitude"] = 123.4          # fuera de rango
    bad["longitude"] = -999.0        # fuera de rango
    bad["event_uid"] = None          # campo obligatorio ausente

    valid, rejected = evaluate_rows([bad])
    assert valid == []
    assert len(rejected) == 1
    rules = {r["rule"] for r in rejected[0].reasons}
    assert "latitude_range" in rules
    assert "longitude_range" in rules
    assert "required_fields" in rules


def test_batch_with_invalid_rows_fails_and_scores_below_100():
    good = _valid_event("ok1")
    bad = _valid_event("bad1")
    bad["mag"] = 99.0                # magnitud implausible
    report = evaluate_batch([good, bad],
                            latest_ingested_at=datetime.now(timezone.utc))
    assert report["records_valid"] == 1
    assert report["records_rejected"] == 1
    assert report["score"] < 100.0
    # la regla de fila fallida hace que el lote quede marcado
    assert report["status"] == "fail"


def test_freshness_rule_detects_stale_data():
    old = datetime.now(timezone.utc) - timedelta(hours=5)
    assert rule_freshness(old, max_age_minutes=30).passed is False
    assert rule_freshness(datetime.now(timezone.utc), 30).passed is True
    assert rule_freshness(None, 30).passed is False


def test_volume_rule_detects_anomalies():
    assert rule_volume(0, 1, 100).passed is False     # volumen bajo
    assert rule_volume(50, 1, 100).passed is True
    assert rule_volume(5000, 1, 100).passed is False  # volumen alto
