# export_geojson.py
import json
from pathlib import Path
import mysql.connector
from mysql.connector import Error

OUT_DIR = Path(__file__).resolve().parent.parent / "www" / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "wells.geojson"

DB_CONFIG = {
    "host": "localhost",
    "user": "devuser",
    "password": "",
    "database": "oil_wells_db"
}

def fetch_wells():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # Basic well info and one-to-many stimulation events joined later
    cursor.execute("""
    SELECT
      w.api_number, w.well_name, w.operator, w.county, w.township_range,
      w.latitude, w.longitude, w.well_status, w.well_type, w.closest_city
    FROM wells w
    WHERE w.latitude IS NOT NULL AND w.longitude IS NOT NULL
    """)
    wells = cursor.fetchall()

    # fetch events grouped by api_number
    cursor.execute("""
    SELECT se.id, se.api_number, se.date_stimulated, se.formation, se.top_ft,
           se.bottom_ft, se.stages, se.total_volume, se.volume_units,
           se.acid_percent, se.lbs_proppant, se.max_pressure_psi,
           se.max_rate_bbl_per_min
    FROM stimulation_events se
    ORDER BY se.api_number, se.id
    """)
    events = cursor.fetchall()

    # fetch proppant details grouped by stimulation_event_id
    cursor.execute("""
    SELECT pd.id, pd.stimulation_event_id, pd.type, pd.volume
    FROM proppant_details pd
    ORDER BY pd.stimulation_event_id, pd.id
    """)
    details = cursor.fetchall()

    cursor.close()
    conn.close()

    # organize events by api_number
    events_by_api = {}
    for e in events:
        events_by_api.setdefault(e["api_number"], []).append(e)

    details_by_event = {}
    for d in details:
        details_by_event.setdefault(d["stimulation_event_id"], []).append({
            "type": d["type"],
            "volume": d["volume"]
        })

    # attach details to events
    for api, evlist in events_by_api.items():
        for ev in evlist:
            ev_id = ev["id"]
            ev["proppant_breakdown"] = details_by_event.get(ev_id, [])

    return wells, events_by_api


def build_geojson():
    wells, events_by_api = fetch_wells()
    features = []

    for w in wells:
        lat = w.get("latitude")
        lon = w.get("longitude")
        if lat is None or lon is None:
            continue

        api = w.get("api_number")
        props = {
            "api_number": api,
            "well_name": w.get("well_name"),
            "operator": w.get("operator"),
            "county": w.get("county"),
            "township_range": w.get("township_range"),
            "well_status": w.get("well_status"),
            "well_type": w.get("well_type"),
            "closest_city": w.get("closest_city"),
            "stimulation_events": events_by_api.get(api, [])
        }

        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "properties": props
        }
        features.append(feature)

    fc = {"type": "FeatureCollection", "features": features}
    with OUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(fc, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(features)} features to {OUT_FILE}")


if __name__ == "__main__":
    build_geojson()