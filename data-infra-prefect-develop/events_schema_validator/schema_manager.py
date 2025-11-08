import os
import logging
from datetime import datetime
from typing import Optional, List

from config import (
    GITHUB_EVENTS_SCHEMA_REPO, 
    GITHUB_SCHEMAS_LOCAL_PATH,
    SCHEMA_VERSION,
    EVENT_ENTITY
)

logger = logging.getLogger('events-schema-validator.schema_manager')


class SchemaManager:
    def __init__(self, github_driver, schemas_local_path: str = GITHUB_SCHEMAS_LOCAL_PATH):
        self.github_driver = github_driver
        self.schemas_local_path = schemas_local_path


    def download_schemas(self) -> bool:
        try:
            logger.info(f"Starting download of schemas from GitHub repository: {GITHUB_EVENTS_SCHEMA_REPO}")
            
            os.makedirs(self.schemas_local_path, exist_ok=True)
            
            self.github_driver.copy_directory(
                repo=GITHUB_EVENTS_SCHEMA_REPO,
                github_path=self.schemas_local_path,
            )

            logger.info(f"Successfully downloaded schemas")
            
            if self._verify_schemas_exist():
                return True
            else:
                logger.error("Schema verification failed - essential files missing")
                return False

        except Exception as e:
            logger.error(f"Failed to download schemas: {e}")
            return False

    def _verify_schemas_exist(self) -> bool:
        essential_files = [
            "event_base_v2.avsc",
        ]
        
        # Add all event schemas from backend v4
        event_schemas = self.get_available_event_schemas()
        essential_files.extend([f"{EVENT_ENTITY}/{SCHEMA_VERSION}/{schema}" for schema in event_schemas])
        
        for file_path in essential_files:
            full_path = os.path.join(self.schemas_local_path, file_path)
            if not os.path.exists(full_path):
                logger.error(f"Essential schema file missing: {full_path}")
                return False
        
        return True

    def get_available_event_schemas(self) -> List[str]:
        """Get all available event schema files from backend v4 directory"""
        event_schemas_path = os.path.join(self.schemas_local_path, EVENT_ENTITY, SCHEMA_VERSION)
        event_schemas = []
        
        try:
            if os.path.exists(event_schemas_path):
                for file in os.listdir(event_schemas_path):
                    if file.endswith('.avsc'):
                        event_schemas.append(file)
            else:
                logger.warning(f"Event schemas directory does not exist: {event_schemas_path}")
        except Exception as e:
            logger.error(f"Failed to list event schemas: {e}")
        
        return sorted(event_schemas)

    def get_schema_path(self, schema_name: str) -> Optional[str]:
        schema_path = os.path.join(self.schemas_local_path, schema_name)
        if os.path.exists(schema_path):
            return schema_path
        else:
            logger.warning(f"Schema file not found: {schema_path}")
            return None

    def list_available_schemas(self) -> list:
        schemas = []
        try:
            for root, dirs, files in os.walk(self.schemas_local_path):
                for file in files:
                    if file.endswith('.avsc'):
                        # Get relative path from schemas directory
                        rel_path = os.path.relpath(os.path.join(root, file), self.schemas_local_path)
                        schemas.append(rel_path)
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
        
        return sorted(schemas)

    def get_schema_info(self) -> dict:
        return {
            'schemas_local_path': self.schemas_local_path,
            'available_schemas': self.list_available_schemas(),
            'available_event_schemas': self.get_available_event_schemas(),
            'essential_schemas_exist': self._verify_schemas_exist()
        }
