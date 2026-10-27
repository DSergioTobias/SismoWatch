"""Prueba unitaria de la función de transformación.

Convierte un evento GeoJSON (forma plana producida por el conector) a fila
de staging: tipado, saneado de textos y conversión de fechas.
"""
from __future__ import annotations

from datetime import datetime, timezone

from transform import event_to_staging, extract_region, sanitize_text


def test_event_to_staging_converts_and_types():
    event = {
        "event_uid": "us7000abcd",
        "mag": "4.6",
        "place": "12 km SW of <b>Foo</b>\x00 City, Chile",
        "event_time": 1766947200000,       # ms epoch
        "updated_time": 1766950800000,
        "tsunami": 1,
        "sig": 88,
        "status": "reviewed",
        "mag_type": "mw",
        "event_type": "earthquake",
        "url": "https://earthquake.usgs.gov/earthquake/1",
        "longitude": -71.5,
        "latitude": -33.1,
        "depth_km": 42.3,
    }

    row = event_to_staging(event, source_id="usgs_feed")

    assert row["source_id"] == "usgs_feed"
    assert row["event_uid"] == "us7000abcd"
    assert row["mag"] == 4.6 and isinstance(row["mag"], float)
    assert isinstance(row["event_time"], datetime)
    assert row["event_time"].tzinfo == timezone.utc
    assert row["tsunami"] is True
    assert row["longitude"] == -71.5
    assert row["latitude"] == -33.1
    assert row["depth_km"] == 42.3
    # el texto se sanea: sin HTML ni caracteres de control
    assert "<b>" not in row["place"]
    assert "\x00" not in row["place"]
    assert row["region"] is not None


def test_event_to_staging_handles_missing_optional_fields():
    row = event_to_staging(
        {"event_uid": "x1", "event_time": 1766947200000,
         "longitude": 0.0, "latitude": 0.0},
        source_id="usgs_feed",
    )
    assert row["mag"] is None
    assert row["place"] is None
    assert row["tsunami"] is False
    assert row["region"] == "unknown"


def test_sanitize_text_and_region():
    assert sanitize_text("  hola\x07  mundo  ") == "hola mundo"
    assert extract_region("12 km SW of Foo, Chile") == "Chile"
    assert extract_region(None) == "unknown"
