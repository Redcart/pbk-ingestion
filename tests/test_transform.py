import pytest
from src.transform import Transform


@pytest.fixture
def transform_instance():
    # Provide an instance of Transform for each test
    return Transform()


def test_clean_data(transform_instance):
    # Test the clean_data method
    raw_data = [{"key": "value"}]
    result = transform_instance.clean_data(raw_data)
    assert result is not None
    assert isinstance(result, list)  # Assuming it returns cleaned data


def test_normalize_data(transform_instance):
    # Test the normalize_data method
    cleaned_data = [{"key": "value"}]
    result = transform_instance.normalize_data(cleaned_data)
    assert result is not None
    assert isinstance(result, list)  # Assuming it returns normalized data


def test_validate_data(transform_instance):
    # Test the validate_data method
    normalized_data = [{"key": "value"}]
    result = transform_instance.validate_data(normalized_data)
    assert result is True  # Assuming it returns True if validation passes
