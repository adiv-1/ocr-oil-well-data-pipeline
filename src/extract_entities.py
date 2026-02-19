import json
import os
import time
from pathlib import Path
from google import genai
from dotenv import load_dotenv
import re

base = Path(__file__).resolve().parent.parent
INPUT = base / "data" / "segments"
OUTPUT = base / "data" / "structured"
LOGS = base / "data" / "logs"

OUTPUT.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

load_dotenv()
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

SECONDS_BETWEEN_CALLS = 2  # stay under rate limit


SYSTEM_PROMPT = """
You are an information extraction engine for oil well regulatory documents.

Extract structured data from the provided document segment.

Return ONLY valid JSON with this exact schema:

{
  "api_number": string or null,
  "well_name": string or null,
  "operator": string or null,
  "county": string or null,
  "township": string or null,
  "range": string or null,
  "section": string or null,
  "latitude": float or null,
  "longitude": float or null,
  "stimulation_events": [
    {
      "date_stimulated": string or null,
      "formation": string or null,
      "top_ft": float or null,
      "bottom_ft": float or null,
      "stages": int or null,
      "total_volume": float or null,
      "volume_units": string or null,
      "treatment_type": string or null,
      "acid_percent": float or null,
      "lbs_proppant": float or null,
      "max_pressure_psi": float or null,
      "max_rate_bbl_per_min": float or null,
      "proppant_breakdown": [
        {
          "type": string or null,
          "volume": float or null
        }
      ]
    }
  ]
}

Rules:
- If a field does not exist, return null.
- If stimulation data does not exist in the segment, return an empty list.
- Convert numeric values to numbers (no commas).
- Do NOT hallucinate!
- Only extract data that clearly appears in the text.
"""


def extract_segment(text, retries=3):
    payload = SYSTEM_PROMPT + "\n\n" + text[:12000]

    for attempt in range(retries):
        try:
            resp = client.models.generate_content(
                model="gemma-3-27b-it",
                contents=payload
            )

            raw = resp.text.strip()

            # remove markdown if model wraps in ```json
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]

            return json.loads(raw)

        except Exception as e:
            msg = str(e)
            match = re.search(r"retry in (\d+\.?\d*)s", msg)
            wait = float(match.group(1)) + 2 if match else 2 ** attempt
            print(f"  rate limited, waiting {wait:.0f}s (attempt {attempt+1})")
            time.sleep(wait)

    return None


def validate(record):
    if not record:
        return False

    lat = record.get("latitude")
    lon = record.get("longitude")

    if lat is not None and not (-90 <= lat <= 90):
        return False

    if lon is not None and not (-180 <= lon <= 180):
        return False

    # validate stimulation numeric values
    stim = record.get("stimulation_events", [])
    if not isinstance(stim, list):
        return False

    return True


def load_existing(output_path):
    if output_path.exists():
        return json.loads(output_path.read_text(encoding="utf-8"))
    return []


def process_file(path):
    well_id = path.stem.replace("_segments", "")
    output_path = OUTPUT / f"{well_id}_structured.json"

    segments = json.loads(path.read_text(encoding="utf-8"))

    results = load_existing(output_path)
    done_ids = {r["segment_id"] for r in results}

    print(f"\nProcessing {well_id} ({len(segments)} segments, {len(done_ids)} already done)")

    for segment in segments:
        seg_id = segment["segment_id"]

        if seg_id in done_ids:
            print(f"  skip segment {seg_id} (already saved)")
            continue

        print(f"  extracting segment {seg_id}...", end=" ", flush=True)

        extracted = extract_segment(segment["text"])

        if validate(extracted):
            results.append({
                "segment_id": seg_id,
                "data": extracted
            })

            output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
            print("saved")
        else:
            print("validation failed")

        time.sleep(SECONDS_BETWEEN_CALLS)

    print(f"done: {output_path.name}")


def main():
    files = sorted(INPUT.glob("*_segments.json"))

    if not files:
        print("No segment files found.")
        return

    for f in files:
        process_file(f)

    print("\nDone.")


if __name__ == "__main__":
    main()