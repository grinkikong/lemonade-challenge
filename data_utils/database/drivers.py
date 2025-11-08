"""Database driver for SQLite (local development).

Note: In production, this would use:
- Metadata: PostgreSQL
- Raw events: Iceberg tables in S3
- Aggregation layer: Snowflake
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from contextlib import contextmanager

from data_utils.database.models import Base, TableMetadata


class DatabaseDriver:
    """Database driver for SQLite (local development only).
    
    Note: In production, this would be replaced with:
    - PostgreSQL for metadata tables
    - Iceberg tables in S3 for raw event data
    - Snowflake for aggregation layer
    """

    def __init__(self, database_url: Optional[str] = None):
        """Initialize database driver.

        Args:
            database_url: Database connection URL. If None, uses DATABASE_URL env var.
                          Must be SQLite for this project.
        """
        # This project uses SQLite only
        # In production: Would use PostgreSQL for metadata
        default_db_path = Path.home() / "data" / "lemonade" / "metadata.db"
        default_db_path.parent.mkdir(parents=True, exist_ok=True)
        default_db_url = f"sqlite:///{default_db_path}"
        self.database_url = database_url or os.environ.get("DATABASE_URL", default_db_url)
        
        # Ensure we're using SQLite
        if not self.database_url.startswith("sqlite"):
            raise ValueError(
                "This project only supports SQLite. "
                "In production, use PostgreSQL for metadata, "
                "Iceberg tables in S3 for raw events, "
                "and Snowflake for aggregation layer."
            )
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None

    @property
    def engine(self) -> Engine:
        """Get or create SQLite database engine.
        
        Note: In production, this would create engine for PostgreSQL.
        """
        if self._engine is None:
            # SQLite only for this project
            # In production: Would use PostgreSQL with appropriate connection pooling
            self._engine = create_engine(
                self.database_url,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
                echo=False,
            )
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False,
            )
        return self._session_factory

    def create_tables(self):
        """Create all tables defined in models.py.
        
        Uses SQLAlchemy Base.metadata.create_all() to generate and execute
        appropriate CREATE TABLE statements for the target database.
        Table definitions are in data_utils.database.models.
        """
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def session(self) -> Session:
        """Context manager for database sessions."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_or_create_table_metadata(
        self,
        table_name: str,
        file_type: str,
        schema_definition: Dict[str, Any],
        partition_column: str = "date",
        processing_config: Dict[str, Any] = None,
        description: str = None,
        status: str = "active",
    ) -> TableMetadata:
        """Get or create table metadata.

        Args:
            table_name: Name of the table.
            file_type: Type of file (vehicle_events or vehicle_status).
            schema_definition: JSON schema definition for Spark processing.
            partition_column: Column name for partitioning.
            processing_config: Additional Spark processing configuration.
            description: Optional description.

        Returns:
            TableMetadata instance.
        """
        with self.session() as session:
            metadata = (
                session.query(TableMetadata)
                .filter(TableMetadata.table_name == table_name)
                .first()
            )
            if not metadata:
                metadata = TableMetadata(
                    table_name=table_name,
                    file_type=file_type,
                    partition_column=partition_column,
                    schema_definition=schema_definition,
                    processing_config=processing_config or {},
                    description=description,
                    status=status,
                )
                session.add(metadata)
                session.commit()
                session.refresh(metadata)
            else:
                # Update if schema changed
                metadata.schema_definition = schema_definition
                if processing_config:
                    metadata.processing_config = processing_config
                if description:
                    metadata.description = description
                if status:
                    metadata.status = status
                session.commit()
                session.refresh(metadata)
            return metadata

    def get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """Get table metadata by table name.

        Args:
            table_name: Name of the table.

        Returns:
            TableMetadata instance or None if not found.
        """
        with self.session() as session:
            return (
                session.query(TableMetadata)
                .filter(TableMetadata.table_name == table_name)
                .first()
            )

    def get_table_metadata_by_file_type(self, file_type: str) -> Optional[TableMetadata]:
        """Get table metadata by file type.

        Args:
            file_type: Type of file (vehicle_events or vehicle_status).

        Returns:
            TableMetadata instance or None if not found.
        """
        with self.session() as session:
            return (
                session.query(TableMetadata)
                .filter(TableMetadata.file_type == file_type)
                .first()
            )

    def create_table_metadata_inactive(
        self,
        file_type: str,
        table_name: str = None,
    ) -> TableMetadata:
        """Create inactive table metadata record when metadata is missing.

        Args:
            file_type: Type of file.
            table_name: Optional table name (defaults to file_type).

        Returns:
            TableMetadata instance with inactive status.
        """
        if table_name is None:
            table_name = file_type

        with self.session() as session:
            metadata = TableMetadata(
                table_name=table_name,
                file_type=file_type,
                partition_column="date",
                schema_definition={},  # Empty schema - needs to be configured
                processing_config={},  # Empty config - needs to be configured
                status="inactive",
                description=f"Auto-created inactive metadata for {file_type} - needs configuration",
            )
            session.add(metadata)
            session.commit()
            session.refresh(metadata)
            return metadata


# Global database driver instance
db_driver = DatabaseDriver()
