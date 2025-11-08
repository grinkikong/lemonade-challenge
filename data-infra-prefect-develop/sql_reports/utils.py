import os
import csv
import shutil
import logging
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)


def create_tmp_dir(path: str) -> None:
    try:
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)
        logger.info(f"Created temporary directory at: {path}")
    except OSError as e:
        logger.error(f"Failed to create temporary directory at {path}: {e}")
        raise


def delete_tmp_dir(path: str) -> None:
    try:
        if os.path.exists(path):
            shutil.rmtree(path)
            logger.info(f"Deleted temporary directory at: {path}")
    except OSError as e:
        logger.error(f"Failed to delete temporary directory at {path}: {e}")


def generate_csv_file(dir_path: str, results: List[Dict], report_name: str) -> str:
    """Generate CSV file from results and return the file path"""
    if not results:
        return None
        
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(dir_path, f"{report_name}_{timestamp}.csv")
    
    try:
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"Generated CSV file at: {file_path}")
        return file_path
    except OSError as e:
        logger.error(f"Failed to generate CSV file at {file_path}: {e}")
        return None
