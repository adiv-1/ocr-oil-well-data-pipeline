import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent.parent
input_dir  = base / "data" / "ocr_pdfs"
output_dir = base / "data" / "segments"
output_dir.mkdir(parents=True, exist_ok=True)


# ── Quality Filter ─────────────────────────────────────────────────────────

def is_garbage(text: str) -> bool:
    """
    Returns True if page should be discarded.
    Uses only text-quality signals — no domain knowledge.
    """
    t = text.strip()

    # Empty or near-empty
    if len(t) < 150:
        return True

    # Garbled image OCR — mostly non-alphabetic
    alpha_ratio = sum(c.isalpha() for c in t) / max(len(t), 1)
    if alpha_ratio < 0.20:
        return True

    # Map pages produce streams of isolated chars
    tokens = t.split()
    if tokens:
        single_char_ratio = sum(1 for tok in tokens if len(tok) == 1) / len(tokens)
        if single_char_ratio > 0.40:
            return True

    return False


# ── Form Start Detection ───────────────────────────────────────────────────

def is_new_form(text: str) -> bool:
    """
    Returns True if this page begins a new logical document.
    Only checks the top 400 characters — headers never appear mid-page.
    """
    header = text.strip()[:400].lower()

    patterns = [
        r"well name and number",
        r"well file\s*(no|number|#)",
        r"api\s*(no|number|#)?[\s:\-]*\d{2}",
        r"sundry notices",
        r"form\s*[468]\b",
        r"sfn\s*\d{4}",
        r"application for permit",
        r"spill\s*(report|incident)",
        r"follow.up spill",
        r"certified survey",
        r"north dakota industrial commission",
        r"oil and gas division",
    ]

    return any(re.search(p, header) for p in patterns)


# ── Segmentation ───────────────────────────────────────────────────────────

def segment_pages(pages: list) -> list:
    """
    Groups consecutive pages into segments.
    A new segment starts whenever is_new_form() triggers.
    """
    segments = []
    current  = []

    for page in pages:
        if is_new_form(page["text"]) and current:
            segments.append(_build_segment(len(segments) + 1, current))
            current = []
        current.append(page)

    if current:
        segments.append(_build_segment(len(segments) + 1, current))

    return segments


def _build_segment(seg_id: int, pages: list) -> dict:
    return {
        "segment_id":   seg_id,
        "page_numbers": [p["page_number"] for p in pages],
        "text":         "\n\n".join(p["text"] for p in pages)
    }


# ── Process Well ───────────────────────────────────────────────────────────

def process_well(json_path: Path) -> None:
    well_id = json_path.stem
    output  = output_dir / f"{well_id}_segments.json"

    if output.exists():
        print(f"  skip: {well_id} (already done)")
        return

    pages = json.loads(json_path.read_text(encoding="utf-8"))
    total = len(pages)

    # Filter garbage
    clean = [p for p in pages if not is_garbage(p["text"])]

    # Segment
    segments = segment_pages(clean)

    # Summary
    print(f"\n{well_id}")
    print(f"  pages    : {total} → {len(clean)} kept ({total - len(clean)} dropped)")
    print(f"  segments : {len(segments)}")
    for s in segments:
        preview = s["text"][:70].replace("\n", " ")
        print(f"    [{s['segment_id']:>2}] pages {str(s['page_numbers']):<15} \"{preview}...\"")

    output.write_text(json.dumps(segments, indent=2), encoding="utf-8")
    print(f"  saved: {output.name}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    wells = sorted(input_dir.glob("*.json"))

    if not wells:
        print(f"No files in {input_dir}")
        return

    print(f"Processing {len(wells)} well(s)\n")
    for w in wells:
        process_well(w)

    print("\nDone.")


if __name__ == "__main__":
    main()