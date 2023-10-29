import datetime
import inspect
import pytest
import pytest_mock
from unittest.mock import Mock
from cdk.common_modules.models.class_time_interval_method import ClassTimeIntervalMethod

@pytest.fixture
def mock_class_instance():
    class MockClass:
        def test(self, start: datetime.datetime, end: datetime.datetime, a: str, b: str = 'test'):
            pass
    return MockClass()

@pytest.fixture
def method_kwargs():
    return {'start': datetime.datetime(2021, 1, 1), 'end': datetime.datetime(2021, 1, 2), 'a': 'test', 'b': 'test'}

def test_check_method_not_exist(mock_class_instance):
    # Create a ClassTimeIntervalMethod instance with a non-existent method name
    with pytest.raises(AttributeError):
        ClassTimeIntervalMethod(class_instance=mock_class_instance, method_name='non_existing_method', method_kwargs={})

def test_valid_input(mock_class_instance,method_kwargs):
    # Create a ClassTimeIntervalMethod instance with a non-existent method name
    assert isinstance(ClassTimeIntervalMethod(class_instance=mock_class_instance, method_name='test', method_kwargs=method_kwargs),ClassTimeIntervalMethod)


def test_check_datetime_method_args_not_exist(mock_class_instance,method_kwargs):
    # Override test method to not have start and end arguments
    mock_class_instance.test = lambda a,b,c: None
    # Create a ClassTimeIntervalMethod instance with a method that doesn't have start and end arguments
    with pytest.raises(ValueError):
        ClassTimeIntervalMethod(class_instance=mock_class_instance, method_name='test', method_kwargs=method_kwargs)


def test_check_redundant_method_kwargs(mock_class_instance,method_kwargs):
    # Add redunant argument to method_kwargs
    method_kwargs['c'] = 'test'

    # Create a ClassTimeIntervalMethod instance with redundant method kwargs
    with pytest.raises(TypeError):
        ClassTimeIntervalMethod(class_instance=mock_class_instance, method_name='test', method_kwargs=method_kwargs)


def test_check_missing_method_kwargs(mock_class_instance,method_kwargs):
    # Remove required argument from method_kwargs
    method_kwargs.pop('a')

    # Create a ClassTimeIntervalMethod instance with missing method kwargs
    with pytest.raises(TypeError):
        ClassTimeIntervalMethod(class_instance=mock_class_instance, method_name='test', method_kwargs=method_kwargs).check_missing_method_kwargs()