"""
PDF Extraction Module

Purpose:
Extract text content from PDF files page by page.
(Generic, adaptive, works for N PDFs)
"""

from typing import List
import fitz  # PyMuPDF
from pathlib import Path
import os
import re

from utils.connect_db import db_connect


def fetch_pdf_from_database(demand_file_id: str) -> bytes:
    """
    Fetch PDF file content from database using stored file path.
    """
    query = """
        SELECT 
            COALESCE(df."filePath", t."filePath") as "filePath"
        FROM "DemandFile" df
        LEFT JOIN "Task" t ON t."demandFileId" = df."id"
        WHERE df."id" = %s
        LIMIT 1
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (demand_file_id,))
        result = cur.fetchone()

        if not result:
            raise ValueError(f"DemandFile with id {demand_file_id} not found")

        file_path = result["filePath"]

        if not file_path:
            raise ValueError(f"File path not found for DemandFile {demand_file_id}")

    pdf_path = None
    attempted_paths = []

    test_path = Path(file_path)
    attempted_paths.append(str(test_path))
    if test_path.exists():
        pdf_path = test_path

    if pdf_path is None:
        uploads_base = os.getenv("UPLOADS_BASE_DIR") or os.getenv("FILE_STORAGE_PATH")
        if uploads_base:
            relative_path = file_path.lstrip("/") if file_path.startswith("/") else file_path
            test_path = Path(uploads_base) / relative_path
            attempted_paths.append(str(test_path))
            if test_path.exists():
                pdf_path = test_path

    if pdf_path is None and file_path.startswith("/"):
        relative_path = file_path.lstrip("/")
        cwd = Path.cwd()

        for level in range(4):
            base = cwd
            for _ in range(level):
                base = base.parent
            test_path = base / relative_path
            attempted_paths.append(str(test_path))
            if test_path.exists():
                pdf_path = test_path
                break

    if pdf_path is None or not pdf_path.exists():
        error_msg = f"PDF file not found. Attempted {len(attempted_paths)} paths:\n"
        for i, path_str in enumerate(attempted_paths[:10], 1):
            error_msg += f"  {i}. {path_str}\n"
        error_msg += f"\nOriginal path from DB: {file_path}\n"
        error_msg += f"Current working directory: {os.getcwd()}\n"
        raise FileNotFoundError(error_msg)

    with open(pdf_path, "rb") as f:
        return f.read()


def extract_text_by_page(pdf_content: bytes) -> List[str]:
    """
    Generic adaptive text extractor.
    Works across digital, hybrid, and noisy PDFs.
    """
    page_texts = []
    pdf_document = fitz.open(stream=pdf_content, filetype="pdf")

    try:
        for page_index in range(len(pdf_document)):
            page = pdf_document[page_index]

            raw_text = page.get_text().strip()

            # Decide extraction path per page
            use_blocks = True

            if _text_density_is_low(raw_text):
                use_blocks = True

            blocks = page.get_text("blocks") if use_blocks else [(0, 0, 0, 0, raw_text)]

            # Stable reading order
            blocks = sorted(blocks, key=lambda b: (b[1], b[0]))

            page_lines = []

            for block in blocks:
                text = block[4]
                if not text:
                    continue

                cleaned = _generic_clean(text)

                if not cleaned:
                    continue

                if _looks_like_garbage(cleaned):
                    continue

                page_lines.append(cleaned)

            page_texts.append("\n".join(page_lines))

    finally:
        pdf_document.close()

    return page_texts


def format_page_text(page_num: int, text: str) -> str:
    """
    Format page text with page markers.
    """
    formatted = f"=== PAGE {page_num} START ===\n"
    formatted += text
    formatted += f"\n=== PAGE {page_num} END ===\n"
    return formatted


# ==================================================
# Internal generic utilities (document-agnostic)
# ==================================================

def _text_density_is_low(text: str) -> bool:
    """
    Detect image-heavy / scanned pages.
    """
    if not text:
        return True
    alpha_chars = sum(c.isalpha() for c in text)
    return alpha_chars < 50


def _generic_clean(text: str) -> str:
    """
    Conservative cleanup safe for all PDFs.
    """
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    text = _normalize_characters(text)
    text = _fix_spacing_patterns(text)

    return text.strip()


def _normalize_characters(text: str) -> str:
    """
    Fix high-confidence character confusions only.
    """
    # | misread as I
    text = re.sub(r"\b\|\b", "I", text)

    # Date-like correction (8/H → 0)
    text = re.sub(r"\b[8H](\d/\d{2}/\d{2,4})\b", r"0\1", text)

    return text


def _fix_spacing_patterns(text: str) -> str:
    """
    Generic spacing fixes without word assumptions.
    """
    # lowercaseUppercase → lowercase Uppercase
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)

    # worddigit → word digit
    text = re.sub(r"([a-zA-Z])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([a-zA-Z])", r"\1 \2", text)

    return text


def _looks_like_garbage(text: str) -> bool:
    """
    Language-agnostic logo / noise detection.
    """
    if len(text) < 5:
        return False

    alpha_ratio = sum(c.isalpha() for c in text) / len(text)
    vowel_ratio = sum(c.lower() in "aeiou" for c in text) / len(text)

    if alpha_ratio < 0.4:
        return True

    if vowel_ratio < 0.2 and len(text) > 12:
        return True

    return False
