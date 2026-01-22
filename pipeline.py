"""
PDF Extraction Pipeline

Purpose:
Combine extraction and directory management modules to process PDF files.
Extracts text from PDFs and saves them to organized directory structure.
"""

from pathlib import Path

from extraction.extraction import (
    fetch_pdf_from_database,
    extract_text_by_page,
    format_page_text,
)
from utils.directory_manager import (
    get_output_base_path,
    create_subdirectories,
    save_text_to_file,
)


def process_pdf_extraction(demand_file_id: str) -> dict:
    """
    Process PDF extraction pipeline for a given demand file.

    Args:
        demand_file_id: DemandFile ID to process

    Returns:
        Dictionary with processing results:
        {
            'success': bool,
            'base_path': Path,
            'pages_extracted': int,
            'files_created': List[str]
        }

    Raises:
        ValueError: If demand_file_id is not found
        FileNotFoundError: If PDF file doesn't exist
        Exception: For other processing errors
    """
    # Step 1: Fetch PDF from database
    print(f"  📥 Fetching PDF for demand_file_id: {demand_file_id}")
    pdf_content = fetch_pdf_from_database(demand_file_id)

    # Step 2: Extract text by page
    print(f"  📄 Extracting text from PDF pages...")
    page_texts = extract_text_by_page(pdf_content)
    num_pages = len(page_texts)
    print(f"  ✓ Extracted {num_pages} pages")

    # Step 3: Get output base path
    print(f"  📁 Getting output directory path...")
    base_path = get_output_base_path(demand_file_id)
    print(f"  ✓ Base path: {base_path}")
    print(f"  ✓ Absolute path: {base_path.resolve()}")

    # Step 4: Create subdirectories
    print(f"  📂 Creating subdirectories...")
    try:
        subdirs = create_subdirectories(base_path)
        raw_extract_dir = subdirs["raw_extract_by_page"]
        print(f"  ✓ Created base directory: {base_path}")
        print(f"  ✓ Created subdirectories: {raw_extract_dir}, {subdirs['chunks']}")
    except Exception as e:
        print(f"  ❌ Failed to create directories: {e}")
        raise

    # Step 5: Format and save each page text
    print(f"  💾 Saving extracted pages to files...")
    files_created = []
    
    for page_index, text in enumerate(page_texts, start=1):
        # Format page text with markers
        formatted_text = format_page_text(page_index, text)
        
        # Generate filename: page_001.txt, page_002.txt, etc.
        filename = f"page_{page_index:03d}.txt"
        
        # Save to raw_extract_by_page directory
        save_text_to_file(raw_extract_dir, filename, formatted_text)
        files_created.append(filename)
    
    print(f"  ✓ Saved {len(files_created)} page files")

    return {
        'success': True,
        'base_path': base_path,
        'pages_extracted': num_pages,   
        'files_created': files_created
    }
