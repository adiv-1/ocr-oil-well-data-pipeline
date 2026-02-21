import os
import json
from pathlib import Path
import fitz
import pytesseract
from PIL import Image

base = Path(__file__).resolve().parent.parent
input_dir = base / "data" / "original_pdfs"
output_dir = base / "data" / "ocr_json"

output_dir.mkdir(parents=True, exist_ok=True)


def extract_pdf_pages(pdf_path):
    doc = fitz.open(pdf_path)
    pages = []

    for i in range(len(doc)):
        # first try reading existing text layer
        text = doc[i].get_text("text").strip()

        # if empty or too short, render as image and OCR directly
        if len(text) < 50:
            pix = doc[i].get_pixmap(matrix=fitz.Matrix(2, 2))
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img = img.convert("L")  # grayscale
            img = img.point(lambda x: 0 if x < 140 else 255)  # binarize
            text = pytesseract.image_to_string(img, config="--psm 6")

        pages.append({
            "page_number": i + 1,
            "text": text.strip()
        })

    doc.close()
    return pages


def main():
    for pdf in input_dir.glob("*.pdf"):
        output_file = output_dir / f"{pdf.stem}.json"

        if output_file.exists():
            print(f"skipping (already extracted): {pdf.name}")
            continue

        print(f"extracting pages from: {pdf.name}")

        try:
            pages = extract_pdf_pages(pdf)

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(pages, f, indent=2)

            print(f"saved {len(pages)} pages: {output_file.name}")

        except Exception as e:
            print(f"failed: {pdf.name} — {e}")


if __name__ == "__main__":
    main()