"""
Storage Interface - Cloud-Agnostic Data Store Abstraction
=========================================================

This module provides a Protocol-based abstraction for data storage,
allowing seamless switching between local (DuckDB) and cloud (S3) backends.

Design Pattern: Strategy Pattern + Dependency Injection
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Protocol, Optional, List
import os


class DataStore(Protocol):
    """
    Abstract storage interface for data operations.
    
    This protocol defines the contract that any storage backend must implement.
    Using Protocol (structural subtyping) instead of ABC for flexibility.
    """
    
    def read(self, key: str) -> bytes:
        """Read data from storage."""
        ...
    
    def write(self, key: str, data: bytes) -> None:
        """Write data to storage."""
        ...
    
    def exists(self, key: str) -> bool:
        """Check if key exists in storage."""
        ...
    
    def list_keys(self, prefix: str = "") -> List[str]:
        """List all keys with given prefix."""
        ...
    
    def delete(self, key: str) -> None:
        """Delete key from storage."""
        ...


class LocalDataStore:
    """
    Local filesystem storage implementation.
    
    Uses local directory structure mimicking S3 key-value pattern.
    Data is stored in: data/{layer}/{filename}
    
    Example:
        store = LocalDataStore(base_path="./data")
        store.write("bronze/players.json", data)
        data = store.read("bronze/players.json")
    """
    
    def __init__(self, base_path: str = "./data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        print(f"📁 LocalDataStore initialized at: {self.base_path.absolute()}")
    
    def read(self, key: str) -> bytes:
        """Read file from local storage."""
        file_path = self.base_path / key
        
        if not file_path.exists():
            raise FileNotFoundError(f"Key not found: {key}")
        
        with open(file_path, 'rb') as f:
            return f.read()
    
    def write(self, key: str, data: bytes) -> None:
        """Write file to local storage."""
        file_path = self.base_path / key
        
        # Create parent directories if they don't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'wb') as f:
            f.write(data)
        
        print(f"  ✓ Wrote: {key} ({len(data):,} bytes)")
    
    def exists(self, key: str) -> bool:
        """Check if file exists."""
        return (self.base_path / key).exists()
    
    def list_keys(self, prefix: str = "") -> List[str]:
        """List all files with given prefix."""
        search_path = self.base_path / prefix
        
        if not search_path.exists():
            return []
        
        # Recursively find all files
        keys = []
        for path in search_path.rglob('*'):
            if path.is_file():
                # Get relative path from base
                relative = path.relative_to(self.base_path)
                keys.append(str(relative))
        
        return sorted(keys)
    
    def delete(self, key: str) -> None:
        """Delete file from local storage."""
        file_path = self.base_path / key
        
        if file_path.exists():
            file_path.unlink()
            print(f"  ✓ Deleted: {key}")


class S3DataStore:
    """
    AWS S3 storage implementation (for production deployment).
    
    Uses boto3 to interact with S3. Requires AWS credentials configured.
    
    Setup:
        1. Install AWS CLI: brew install awscli
        2. Configure credentials: aws configure
        3. Set environment variables:
           - AWS_ACCESS_KEY_ID
           - AWS_SECRET_ACCESS_KEY
           - AWS_DEFAULT_REGION
    
    Example:
        store = S3DataStore(bucket="nba-goat-data-lake")
        store.write("bronze/players.json", data)
        data = store.read("bronze/players.json")
    
    Note: Currently a placeholder. Will be implemented in Week 5.
    """
    
    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.region = region
        self._client = None
        
        print(f"☁️  S3DataStore initialized for bucket: {bucket}")
        print("⚠️  Note: S3 backend not yet implemented. Using local fallback.")
    
    @property
    def client(self):
        """Lazy-load S3 client (only when needed)."""
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client('s3', region_name=self.region)
                print(f"  ✓ Connected to S3 in region: {self.region}")
            except ImportError:
                raise ImportError(
                    "boto3 not installed. Install with: pip install boto3"
                )
            except Exception as e:
                raise ConnectionError(
                    f"Failed to connect to S3: {e}\n"
                    "Make sure AWS credentials are configured."
                )
        return self._client
    
    def read(self, key: str) -> bytes:
        """Read object from S3."""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()
        except Exception as e:
            raise FileNotFoundError(f"Key not found in S3: {key}") from e
    
    def write(self, key: str, data: bytes) -> None:
        """Write object to S3."""
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ServerSideEncryption='AES256'  # Encrypt at rest
            )
            print(f"  ✓ Wrote to S3: {key} ({len(data):,} bytes)")
        except Exception as e:
            raise IOError(f"Failed to write to S3: {key}") from e
    
    def exists(self, key: str) -> bool:
        """Check if object exists in S3."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except:
            return False
    
    def list_keys(self, prefix: str = "") -> List[str]:
        """List all objects with given prefix."""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
        except Exception as e:
            raise IOError(f"Failed to list S3 keys: {e}") from e
    
    def delete(self, key: str) -> None:
        """Delete object from S3."""
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
            print(f"  ✓ Deleted from S3: {key}")
        except Exception as e:
            raise IOError(f"Failed to delete from S3: {key}") from e


def get_storage(storage_type: Optional[str] = None) -> DataStore:
    """
    Factory function to get appropriate storage backend.
    
    Args:
        storage_type: "local" or "s3". If None, reads from STORAGE_TYPE env var.
    
    Returns:
        DataStore implementation (LocalDataStore or S3DataStore)
    
    Environment Variables:
        STORAGE_TYPE: "local" or "s3" (default: "local")
        S3_BUCKET: S3 bucket name (required if storage_type="s3")
        AWS_REGION: AWS region (default: "us-east-1")
    
    Example:
        # Development (local)
        export STORAGE_TYPE=local
        storage = get_storage()
        
        # Production (S3)
        export STORAGE_TYPE=s3
        export S3_BUCKET=nba-goat-data-lake
        storage = get_storage()
    """
    
    if storage_type is None:
        storage_type = os.getenv("STORAGE_TYPE", "local")
    
    if storage_type == "local":
        base_path = os.getenv("DATA_PATH", "./data")
        return LocalDataStore(base_path=base_path)
    
    elif storage_type == "s3":
        bucket = os.getenv("S3_BUCKET")
        if not bucket:
            raise ValueError(
                "S3_BUCKET environment variable required for S3 storage"
            )
        
        region = os.getenv("AWS_REGION", "us-east-1")
        return S3DataStore(bucket=bucket, region=region)
    
    else:
        raise ValueError(
            f"Unknown storage type: {storage_type}. "
            f"Must be 'local' or 's3'"
        )


# Example usage
if __name__ == "__main__":
    print("\n=== Testing Storage Interface ===\n")
    
    # Test local storage
    print("1. Testing LocalDataStore:")
    local_store = get_storage("local")
    
    # Write test data
    test_data = b"Hello, NBA GOAT Index!"
    local_store.write("test/example.txt", test_data)
    
    # Read back
    retrieved = local_store.read("test/example.txt")
    assert retrieved == test_data, "Data mismatch!"
    
    # List keys
    keys = local_store.list_keys("test")
    print(f"  Found keys: {keys}")
    
    # Clean up
    local_store.delete("test/example.txt")
    
    print("\n✅ LocalDataStore tests passed!\n")
    
    # Note about S3
    print("2. S3DataStore:")
    print("   ⚠️  S3 implementation ready for Week 5 deployment.")
    print("   ⚠️  Set STORAGE_TYPE=s3 to use cloud storage.\n")
