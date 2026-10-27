-- ============================================================
-- 05_seed.sql - Fuente por defecto: USGS Earthquake Catalog
-- El catálogo de fuentes permite añadir cualquier API sin tocar
-- el código del pipeline: insertar una fila + su mapeo en config.
-- ============================================================
INSERT INTO ops.sources (source_id, name, connector_type, config, schedule, is_active)
VALUES (
    'usgs_feed',
    'USGS Earthquake Catalog (feed horario)',
    'usgs',
    '{
        "realtime_url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        "historic_url": "https://earthquake.usgs.gov/fdsnws/event/1/query",
        "timeout_seconds": 30,
        "max_retries": 3,
        "field_map": {
            "event_uid": "id",
            "mag": "properties.mag",
            "place": "properties.place",
            "event_time": "properties.time",
            "updated_time": "properties.updated",
            "tsunami": "properties.tsunami",
            "sig": "properties.sig",
            "status": "properties.status",
            "mag_type": "properties.magType",
            "event_type": "properties.type",
            "url": "properties.url",
            "longitude": "geometry.coordinates[0]",
            "latitude": "geometry.coordinates[1]",
            "depth_km": "geometry.coordinates[2]"
        }
    }'::jsonb,
    '*/5 * * * *',
    true
)
ON CONFLICT (source_id) DO NOTHING;
