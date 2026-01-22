"""
PDF Extraction Module

Purpose:
Extract text content from PDF files page by page.
"""

from typing import List
import fitz  # PyMuPDF
from pathlib import Path
import os

from utils.connect_db import db_connect


def fetch_pdf_from_database(demand_file_id: str) -> bytes:
    """
    Fetch PDF file content from database using stored file path.

    Args:
        demand_file_id: DemandFile ID

    Returns:
        PDF file content as bytes

    Raises:
        FileNotFoundError: If the file path doesn't exist
        ValueError: If demand_file_id is not found in database
    """
    # Query database for file path from DemandFile table
    # Prefer DemandFile.filePath, fall back to Task.filePath if DemandFile.filePath is null
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

    # Resolve PDF file path
    # Handle both absolute (Unix-style) and relative paths
    pdf_path = None
    attempted_paths = []
    
    # Strategy 1: Try path as-is (works for absolute Windows paths and existing files)
    test_path = Path(file_path)
    attempted_paths.append(str(test_path))
    if test_path.exists():
        pdf_path = test_path
    
    # Strategy 2: Check environment variable for base uploads directory
    if pdf_path is None:
        uploads_base = os.getenv('UPLOADS_BASE_DIR') or os.getenv('FILE_STORAGE_PATH')
        if uploads_base:
            if file_path.startswith('/'):
                relative_path = file_path.lstrip('/')
            else:
                relative_path = file_path
            test_path = Path(uploads_base) / relative_path
            attempted_paths.append(str(test_path))
            if test_path.exists():
                pdf_path = test_path
    
    # Strategy 3: If path starts with '/' (Unix absolute) on Windows, resolve relative to various bases
    if pdf_path is None and file_path.startswith('/'):
        relative_path = file_path.lstrip('/')
        
        # Try relative to current working directory
        test_path = Path(relative_path)
        attempted_paths.append(str(test_path))
        if test_path.exists():
            pdf_path = test_path
        
        # Try relative to CWD
        if pdf_path is None:
            cwd = Path.cwd()
            test_path = cwd / relative_path
            attempted_paths.append(str(test_path))
            if test_path.exists():
                pdf_path = test_path
        
        # Try parent directories (go up to 3 levels)
        if pdf_path is None:
            current = Path.cwd()
            for level in range(4):  # 0, 1, 2, 3 levels up
                base = current
                for _ in range(level):
                    base = base.parent
                test_path = base / relative_path
                attempted_paths.append(str(test_path))
                if test_path.exists():
                    pdf_path = test_path
                    break
        
        # Try common sibling directories (if CWD is in a subdirectory)
        if pdf_path is None:
            cwd = Path.cwd()
            # Check if we're in a subdirectory and try parent/../uploads
            if 'legasys-dev' in str(cwd) or 'legasys' in str(cwd):
                # Try going up and looking for uploads
                for level in range(1, 4):
                    base = cwd
                    for _ in range(level):
                        base = base.parent
                    # Try base/uploads/...
                    test_path = base / relative_path
                    attempted_paths.append(str(test_path))
                    if test_path.exists():
                        pdf_path = test_path
                        break
                    # Try base/../uploads/...
                    test_path = base.parent / relative_path
                    attempted_paths.append(str(test_path))
                    if test_path.exists():
                        pdf_path = test_path
                        break
    
    # If file still not found, raise error with helpful information
    if pdf_path is None or not pdf_path.exists():
        error_msg = (
            f"PDF file not found. Attempted {len(attempted_paths)} paths:\n"
        )
        for i, path_str in enumerate(attempted_paths[:10], 1):  # Show first 10
            error_msg += f"  {i}. {path_str}\n"
        if len(attempted_paths) > 10:
            error_msg += f"  ... and {len(attempted_paths) - 10} more paths\n"
        error_msg += f"\nOriginal path from DB: {file_path}\n"
        error_msg += f"Current working directory: {os.getcwd()}\n"
        error_msg += f"\nTip: Set UPLOADS_BASE_DIR environment variable to the base directory containing 'uploads' folder."
        
        raise FileNotFoundError(error_msg)

    with open(pdf_path, "rb") as f:
        return f.read()


def extract_text_by_page(pdf_content: bytes) -> List[str]:
    """
    Extract text from PDF content page by page.

    Args:
        pdf_content: PDF file content as bytes

    Returns:
        List of extracted page texts (1 element per page)
    """
    page_texts = []

    # Open PDF from bytes
    pdf_document = fitz.open(stream=pdf_content, filetype="pdf")

    try:
        # Extract text from each page
        for page_num in range(len(pdf_document)):
            page = pdf_document[page_num]
            text = page.get_text()
            page_texts.append(text)
    finally:
        pdf_document.close()

    return page_texts


def format_page_text(page_num: int, text: str) -> str:
    """
    Format page text with page markers.

    Args:
        page_num: Page number (1-indexed)
        text: Extracted text from page

    Returns:
        Formatted page text with markers
    """
    formatted = f"=== PAGE {page_num} START ===\n"
    formatted += text
    formatted += f"\n=== PAGE {page_num} END ===\n"
    return formatted
