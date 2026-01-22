"""
Directory Management Module

Purpose:
Create and manage output directory structure using DB-provided output path.
"""

from pathlib import Path
from typing import Dict
import os
import re

from utils.connect_db import db_connect


def get_output_base_path(demand_file_id: str) -> Path:
    """
    Get output base directory path from database.

    Args:
        demand_file_id: DemandFile ID

    Returns:
        Path object for the base output directory
        Format: outputs/<job_id>/<file_name>/

    Raises:
        ValueError: If demand_file_id is not found in database
    """
    # Query database for job_id, file_name, and outputFilePath
    # Get job_id from Task, file_name from Task or DemandFile
    query = """
        SELECT 
            t."jobId",
            COALESCE(t."fileName", df."fileName") as "fileName",
            t."outputFilePath"
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

        job_id = result.get("jobId")
        file_name = result.get("fileName")
        output_file_path = result.get("outputFilePath")
        
        if not job_id:
            raise ValueError(f"Job ID not found for DemandFile {demand_file_id}")
        if not file_name:
            raise ValueError(f"File name not found for DemandFile {demand_file_id}")

    # Resolve output directory path using similar strategy as PDF path resolution
    # Note: Unlike PDF paths, we resolve output paths for creation, not for finding existing files
    # Structure: outputs/<job_id>/<file_name>/
    
    # Sanitize file_name for filesystem (remove invalid characters)
    sanitized_file_name = re.sub(r'[<>:"/\\|?*]', '_', file_name)
    # Remove file extension if present for directory name
    if '.' in sanitized_file_name:
        sanitized_file_name = sanitized_file_name.rsplit('.', 1)[0]
    
    base_path = None
    
    if output_file_path:
        # Strategy 1: Check environment variable for base outputs directory (highest priority)
        outputs_base = os.getenv('OUTPUTS_BASE_DIR') or os.getenv('FILE_STORAGE_PATH')
        if outputs_base:
            if output_file_path.startswith('/'):
                relative_path = output_file_path.lstrip('/')
            else:
                relative_path = output_file_path
            base_path = (Path(outputs_base) / relative_path).resolve()
        
        # Strategy 2: If path is absolute, use it as-is
        elif Path(output_file_path).is_absolute():
            base_path = Path(output_file_path).resolve()
        
        # Strategy 3: If path starts with '/' (Unix absolute) on Windows, resolve relative to CWD
        elif output_file_path.startswith('/') and os.name == 'nt':
            relative_path = output_file_path.lstrip('/')
            base_path = (Path.cwd() / relative_path).resolve()
        
        # Strategy 4: Try parent directories (go up to 3 levels) for Unix-style paths
        elif output_file_path.startswith('/'):
            relative_path = output_file_path.lstrip('/')
            current = Path.cwd()
            # Try current directory first
            base_path = (current / relative_path).resolve()
        else:
            # Relative path - resolve relative to current working directory
            base_path = (Path.cwd() / output_file_path).resolve()
    else:
        # Construct path: outputs/<job_id>/<file_name>/
        constructed_path = f"outputs/{job_id}/{sanitized_file_name}"
        
        # Strategy 1: Check environment variable for base outputs directory
        outputs_base = os.getenv('OUTPUTS_BASE_DIR') or os.getenv('FILE_STORAGE_PATH')
        if outputs_base:
            base_path = (Path(outputs_base) / constructed_path).resolve()
        else:
            # Default: relative to current working directory
            base_path = (Path.cwd() / constructed_path).resolve()
    
    return base_path


def get_job_output_directory(job_id: str) -> Path:
    """
    Get the job-level output directory path.
    
    Args:
        job_id: Job ID
        
    Returns:
        Path object for the job output directory: outputs/<job_id>/
    """
    # Check environment variable for base outputs directory
    outputs_base = os.getenv('OUTPUTS_BASE_DIR') or os.getenv('FILE_STORAGE_PATH')
    if outputs_base:
        job_dir = (Path(outputs_base) / "outputs" / job_id).resolve()
    else:
        # Default: relative to current working directory
        job_dir = (Path.cwd() / "outputs" / job_id).resolve()
    
    # Create job directory if it doesn't exist
    job_dir.mkdir(parents=True, exist_ok=True)
    
    return job_dir


def create_subdirectories(base_dir: Path) -> Dict[str, Path]:
    """
    Create subdirectories for raw extracts and chunks.

    Args:
        base_dir: Base directory path

    Returns:
        Dictionary with keys 'raw_extract_by_page' and 'chunks',
        containing Path objects for each subdirectory
    """
    # Ensure base_dir is a resolved absolute path
    base_dir = base_dir.resolve()
    
    # Create base directory if it doesn't exist
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise OSError(f"Failed to create base directory {base_dir}: {e}")

    # Create subdirectories
    raw_extract_dir = base_dir / "raw_extract_by_page"
    chunks_dir = base_dir / "chunks"

    try:
        raw_extract_dir.mkdir(parents=True, exist_ok=True)
        chunks_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise OSError(f"Failed to create subdirectories in {base_dir}: {e}")

    return {
        "raw_extract_by_page": raw_extract_dir,
        "chunks": chunks_dir
    }


def save_text_to_file(directory: Path, filename: str, content: str) -> None:
    """
    Save text content to a file in UTF-8 encoding.

    Args:
        directory: Directory path where file should be saved
        filename: Name of the file to create
        content: Text content to write

    Note:
        This function is idempotent - overwriting existing files is allowed.
    """
    # Ensure directory exists
    directory.mkdir(parents=True, exist_ok=True)

    # Create full file path
    file_path = directory / filename

    # Write content in UTF-8 encoding
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
