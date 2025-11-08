"""S3 driver for file operations (local filesystem for dev, S3 for production)."""

import os
import shutil
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError


class S3Driver:
    """S3 driver for file operations."""

    def __init__(self):
        """Initialize S3 driver."""
        self.env = os.environ.get("ENV", "local")
        self.is_local = self.env == "local"

        if not self.is_local:
            self.s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
            self.bucket_name = os.environ.get("S3_BUCKET")
            if not self.bucket_name:
                raise ValueError("S3_BUCKET environment variable required for production")

    def exists(self, path: str) -> bool:
        """Check if path exists (file or directory/prefix).

        Args:
            path: Path to check (local or S3 key/prefix).

        Returns:
            True if path exists, False otherwise.
        """
        if self.is_local:
            return Path(path).exists()
        else:
            # For S3, check if prefix exists by listing
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=path, MaxKeys=1
                )
                return "Contents" in response and len(response.get("Contents", [])) > 0
            except ClientError:
                return False

    def is_dir(self, path: str) -> bool:
        """Check if path is a directory (or S3 prefix with objects).

        Args:
            path: Path to check (local or S3 prefix).

        Returns:
            True if path is a directory/prefix, False otherwise.
        """
        if self.is_local:
            return Path(path).is_dir()
        else:
            # For S3, check if it's a prefix (ends with / or has objects)
            if path.endswith("/"):
                return True
            # Check if there are any objects with this prefix
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=path, Delimiter="/", MaxKeys=1
                )
                return "Contents" in response or "CommonPrefixes" in response
            except ClientError:
                return False

    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix.

        Args:
            prefix: Prefix to filter files (folder path).

        Returns:
            List of file paths/keys.
        """
        if self.is_local:
            # Local filesystem
            base_path = Path(prefix)
            # Resolve to absolute path (works for both relative and absolute)
            if not base_path.is_absolute():
                base_path = base_path.resolve()
            # Ensure directory exists
            if not base_path.exists():
                return []
            if not base_path.is_dir():
                return []
            # List JSON files
            json_files = list(base_path.glob("*.json"))
            return [str(f) for f in json_files]
        else:
            # S3 - list objects with the prefix
            try:
                # Ensure prefix ends with / for directory-like listing
                s3_prefix = prefix if prefix.endswith("/") else f"{prefix}/"
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=s3_prefix
                )
                # Filter out "directory" markers (keys ending with /)
                files = [
                    obj["Key"]
                    for obj in response.get("Contents", [])
                    if not obj["Key"].endswith("/") and obj["Key"].endswith(".json")
                ]
                return files
            except ClientError as e:
                print(f"Error listing S3 objects: {e}")
                return []

    def read_file(self, file_path: str) -> bytes:
        """Read file contents.

        Args:
            file_path: Path to file (local) or S3 key.

        Returns:
            File contents as bytes.
        """
        if self.is_local:
            return Path(file_path).read_bytes()
        else:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_path)
                return response["Body"].read()
            except ClientError as e:
                raise Exception(f"Error reading S3 object {file_path}: {e}")

    def move_file(self, source_path: str, dest_path: str):
        """Move file from source to destination.

        Args:
            source_path: Source file path/key.
            dest_path: Destination file path/key.
        """
        if self.is_local:
            # Local filesystem move
            source = Path(source_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(dest))
        else:
            # S3 copy + delete
            try:
                self.s3_client.copy_object(
                    CopySource={"Bucket": self.bucket_name, "Key": source_path},
                    Bucket=self.bucket_name,
                    Key=dest_path,
                )
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=source_path)
            except ClientError as e:
                raise Exception(f"Error moving S3 object {source_path} to {dest_path}: {e}")

    def write_file(self, file_path: str, content: bytes):
        """Write file content.

        Args:
            file_path: Destination file path/key.
            content: File content as bytes.
        """
        if self.is_local:
            dest = Path(file_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
        else:
            try:
                self.s3_client.put_object(Bucket=self.bucket_name, Key=file_path, Body=content)
            except ClientError as e:
                raise Exception(f"Error writing S3 object {file_path}: {e}")

    def create_folder(self, folder_path: str):
        """Create folder/directory if it doesn't exist.

        Args:
            folder_path: Path to folder to create.
        """
        if self.is_local:
            Path(folder_path).mkdir(parents=True, exist_ok=True)
        else:
            # For S3, folders don't need to be explicitly created
            # They exist implicitly when objects are placed under a prefix
            # But we can create a placeholder object to ensure the prefix exists
            if not folder_path.endswith("/"):
                folder_path = f"{folder_path}/"
            # Create a placeholder file to ensure the prefix exists
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=f"{folder_path}.gitkeep",
                    Body=b""
                )
            except ClientError as e:
                raise Exception(f"Error creating S3 folder {folder_path}: {e}")


# Global S3 driver instance
s3_driver = S3Driver()

