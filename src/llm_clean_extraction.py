import re
import json
import os
import requests
from google import genai 
from dotenv import load_dotenv
from pathlib import Path
from pydantic import BaseModel, Field 
from typing import List, Optional 

base = Path(__file__).resolve().parent.parent
INPUT = base / "data" / "structured"
OUTPUT = base / "data" / "final_outputs"

OUTPUT.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Define Classes for LLM Output
# -----------------------------

class Details(BaseModel):
    type: Optional[str]
    volume: Optional[float]


class StimEvents(BaseModel):
    date_stimulated: Optional[str]
    formation: Optional[str]
    top_ft: Optional[float]
    bottom_ft: Optional[float]
    stages: Optional[int]
    total_volume: Optional[float]
    volume_units: Optional[str]
    acid_percent: Optional[float]
    lbs_proppant: Optional[float]
    max_pressure_psi: Optional[float]
    max_rate_bbl_per_min: Optional[float]
    proppant_breakdown: Optional[List[Details]]


class OilWell(BaseModel):
    api_number: Optional[str]
    well_name: Optional[str]
    operator: Optional[str]
    county: Optional[str]
    township_range: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    stimulation_events: Optional[List[StimEvents]]

# -----------------------------
# Format Validation Functions
# -----------------------------

def validate_api(api):
    if not api:
        return None

    # Remove everything except digits
    digits = re.sub(r"\D", "", str(api))

    if len(digits) != 10:
        return None

    # Reformat into "42-503-12345" style
    return f"{digits[0:2]}-{digits[2:5]}-{digits[5:]}"


def combine_township_range(township, range_):
    """
    Combine township and range into proper format.
    Returns None if both are missing.
    """
    if isinstance(township, str) and isinstance(range_, str):
        township = township.strip() if township else None
        range_ = range_.strip() if range_ else None

        if township and range_:
            return f"{township}, {range_}"
        elif township:
            return township
        elif range_:
            return range_
        else: 
            return None
    else:
        return None


def validate_lat_lon(value):
    if not value:
        return None
    try:
        val = float(value)
        if -180 <= val <= 180:
            return val
    except:
        pass
    return None


# -----------------------------
# Preprocess Candidates
# -----------------------------

def clean_segments(segments):
    cleaned = []

    for segment in segments:
        data = segment["data"]

        cleaned_data = {
            "api_number": validate_api(data.get("api_number")),
            "well_name": data.get("well_name"),
            "operator": data.get("operator"),
            "county": data.get("county"),
            "township_range": combine_township_range(data.get("township"), data.get("range")),
            "latitude": validate_lat_lon(data.get("latitude")),
            "longitude": validate_lat_lon(data.get("longitude")),
            "stimulation_events": data.get("stimulation_events", [])
        }

        cleaned.append({
            "segment_id": segment["segment_id"],
            "data": cleaned_data
        })

    return cleaned

def process(input_path, output_path):
    for json_file in sorted(input_path.glob("*.json")):
        print(f"\nProcessing: {json_file.name}")

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                segments = json.load(f)

            # Clean segments (your rule-based validator)
            cleaned_segments = clean_segments(segments)

            # Send to Gemini
            final_json_text = reconcile_with_gemini(cleaned_segments)

            # Save finalized output
            output_file = output_path / json_file.name

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(final_json_text.model_dump(), f, indent=2)

            print("Successfully finalized")

        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
            


load_dotenv()
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

def reconcile_with_gemini(cleaned_segments):
    prompt = f"""
You are given multiple segments (based on segment_id) with candidate data extracted from a scanned oil well PDF.

Each segment may contain correct, partial, or incorrect values.

Your task:
1. Compare all candidate segments.
2. Choose the most consistent and complete value for fields: "api_number", "well_name", "operator", "county", "township_range", "latitude", and "longitude".
3. For the last field "stimulation_events", please append from each segment if "stimulation_events" is not an empty list.
4. Return a SINGLE final JSON object.
5. If unsure, return null for that field.
6. Normalize casing (e.g., proper title case for county, uppercase for operator).

Candidate Segments:
{json.dumps(cleaned_segments, indent=2)}

Return ONLY valid JSON.
"""

    response = client.models.generate_content(
        model="gemini-3-flash-preview",
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_schema": OilWell.model_json_schema()
        })
    result = OilWell.model_validate_json(response.text)
    return result

if __name__ == "__main__":
    process(
        input_path=INPUT,
        output_path=OUTPUT
    )