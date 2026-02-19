import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent.parent
INPUT = base / "data" / "ocr_pdfs"
OUT = base / "data" / "segments"
OUT.mkdir(parents=True, exist_ok=True)

MIN_LEN = 150
ALPHA_RATIO = 0.20
SINGLE_CHAR_RATIO = 0.40


def is_garbage(text):
    t = text.strip()
    if len(t) < MIN_LEN:
        return True
    alpha = sum(1 for c in t if c.isalpha()) / max(len(t), 1)
    if alpha < ALPHA_RATIO:
        return True
    toks = t.split()
    if toks:
        single = sum(1 for tok in toks if len(tok) == 1) / len(toks)
        if single > SINGLE_CHAR_RATIO:
            return True
    return False


def is_new_form(text):
    h = text.strip()[:400].lower()
    patterns = [
        r"well name and number",
        r"well file\s*(no|number|#)",
        r"api\s*(no|number|#)?[\s:\-]*\d{2}",
        r"sundry notices",
        r"form\s*[468]\b",
        r"sfn\s*\d{3,5}",
        r"spill\s*(report|incident)",
        r"certified survey",
        r"north dakota industrial commission",
        r"oil and gas division",
    ]
    for p in patterns:
        if re.search(p, h):
            return True
    return False


def make_segment(i, pages):
    return {
        "segment_id": i,
        "page_numbers": [p["page_number"] for p in pages],
        "text": "\n\n".join(p["text"] for p in pages)
    }


def segment_pages(pages):
    segs = []
    cur = []
    for p in pages:
        if is_new_form(p["text"]) and cur:
            segs.append(make_segment(len(segs) + 1, cur))
            cur = []
        cur.append(p)
    if cur:
        segs.append(make_segment(len(segs) + 1, cur))
    return segs


def process_file(path):
    name = path.stem
    outp = OUT / f"{name}_segments.json"
    if outp.exists():
        print("skip:", name)
        return
    pages = json.loads(path.read_text(encoding="utf-8"))
    total = len(pages)
    clean = [p for p in pages if not is_garbage(p["text"])]
    segs = segment_pages(clean)
    print(f"\n{name}")
    print(f" pages: {total} -> kept {len(clean)} dropped {total - len(clean)}")
    print(f" segments: {len(segs)}")
    for s in segs:
        preview = s["text"][:70].replace("\n", " ")
        print(f"  [{s['segment_id']}] pages {s['page_numbers']} \"{preview}...\"")
    outp.write_text(json.dumps(segs, indent=2), encoding="utf-8")
    print("saved:", outp.name)


def main():
    files = sorted(INPUT.glob("*.json"))
    if not files:
        print("no input files in", INPUT)
        return
    for f in files:
        process_file(f)


if __name__ == "__main__":
    main()