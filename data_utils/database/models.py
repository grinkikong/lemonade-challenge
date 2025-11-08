"""SQLAlchemy models for table metadata configuration.

Stores table metadata and schema definitions for Spark processing.
"""

from datetime import datetime
from sqlalchemy import Column, String, DateTime, Integer, Text, JSON, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class TableMetadata(Base):
    """Table metadata configuration for Spark processing.

    Stores schema definitions and processing configuration for each table.
    Each row represents metadata for a table name.
    """

    __tablename__ = "table_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(100), nullable=False, unique=True, index=True)
    file_type = Column(String(50), nullable=False, index=True)  # vehicle_events or vehicle_status
    partition_column = Column(String(50), default="date")
    
    # Processing folder path (relative to PROCESSING_FOLDER base)
    # Example: "vehicle_events" or "vehicle_status"
    # Full path will be: {PROCESSING_FOLDER}/{processing_folder_path}/
    processing_folder_path = Column(String(255), nullable=True)  # If None, defaults to file_type
    
    # Schema definition for Spark (JSON structure)
    # Defines the structure/fields for the payload/events
    schema_definition = Column(JSON, nullable=False)  # JSON schema or field definitions
    
    # Processing configuration for Spark
    processing_config = Column(JSON, nullable=True)  # Spark processing options
    
    # Status: active, inactive
    status = Column(String(20), default="active", nullable=False, index=True)
    
    # Metadata
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_table_metadata_file_type", "file_type"),
    )

    def __repr__(self):
        return f"<TableMetadata(table_name='{self.table_name}', file_type='{self.file_type}')>"
