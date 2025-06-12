import pytest
from src.extract import Extract


@pytest.fixture
def extract_instance():
    # Provide an instance of Extract for each test
    return Extract()


def test_load_data(extract_instance):
    # Test the load_data method
    result = extract_instance.load_data("test_input_file")
    assert result is not None
    assert isinstance(result, list)


def test_transform_data(extract_instance):
    # Test the transform_data method
    input_data = [{"key": "value"}]
    result = extract_instance.transform_data(input_data)
    assert result is not None
    assert isinstance(result, list)


def test_save_data(extract_instance):
    # Test the save_data method
    data_to_save = [{"key": "value"}]
    result = extract_instance.save_data(data_to_save, "test_output_file")
    assert result is True
