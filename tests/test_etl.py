import pytest
from src.etl import ETL


@pytest.fixture
def etl_instance():
    # Provide an instance of ETL for each test
    return ETL()


def test_extract(etl_instance):
    # Test the extract method
    result = etl_instance.extract("test_source")
    assert result is not None
    assert isinstance(result, list)


def test_transform(etl_instance):
    # Test the transform method
    input_data = [{"key": "value"}]
    result = etl_instance.transform(input_data)
    assert result is not None
    assert isinstance(result, list)


def test_load(etl_instance):
    # Test the load method
    data_to_load = [{"key": "value"}]
    result = etl_instance.load(data_to_load, "test_destination")
    assert result is True
