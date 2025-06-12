import pytest
from src.ingest import Ingest


@pytest.fixture
def ingest_instance():
    # Provide an instance of Ingest for each test
    return Ingest()


def test_read_source(ingest_instance):
    # Test the read_source method
    result = ingest_instance.read_source("test_source_file")
    assert result is not None
    assert isinstance(result, list)  # Assuming it returns a list of data


def test_process_data(ingest_instance):
    # Test the process_data method
    input_data = [{"key": "value"}]
    result = ingest_instance.process_data(input_data)
    assert result is not None
    assert isinstance(result, list)  # Assuming it returns processed data


def test_write_output(ingest_instance):
    # Test the write_output method
    data_to_write = [{"key": "value"}]
    result = ingest_instance.write_output(data_to_write, "test_output_file")
    assert result is True  # Assuming it returns True on success
