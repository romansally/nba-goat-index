"""Tests for storage interface."""

import pytest
from pathlib import Path
from src.storage.storage_interface import LocalDataStore, get_storage


def test_local_storage_write_read(tmp_path):
    """Test basic write and read operations."""
    store = LocalDataStore(base_path=str(tmp_path))
    
    # Write data
    test_data = b"Test NBA data"
    store.write("test/sample.txt", test_data)
    
    # Read back
    retrieved = store.read("test/sample.txt")
    
    assert retrieved == test_data


def test_local_storage_exists(tmp_path):
    """Test file existence check."""
    store = LocalDataStore(base_path=str(tmp_path))
    
    # Initially doesn't exist
    assert not store.exists("test/nonexistent.txt")
    
    # After writing, exists
    store.write("test/exists.txt", b"data")
    assert store.exists("test/exists.txt")


def test_local_storage_list_keys(tmp_path):
    """Test listing keys with prefix."""
    store = LocalDataStore(base_path=str(tmp_path))
    
    # Write multiple files
    store.write("bronze/file1.json", b"data1")
    store.write("bronze/file2.json", b"data2")
    store.write("silver/file3.json", b"data3")
    
    # List bronze files
    bronze_keys = store.list_keys("bronze")
    assert len(bronze_keys) == 2
    assert "bronze/file1.json" in bronze_keys


def test_get_storage_factory():
    """Test storage factory function."""
    store = get_storage("local")
    assert isinstance(store, LocalDataStore)


# Add more tests as you build...
