from typing import Any
import pytest
import requests
import datetime

from cdk.services.api.el_overblik import ElOverblik, ElOverblikException, ElOverblikTooManyRequestsException, ElOverblikServiceUnavailableException
from cdk.common_modules.access.secrets import Secrets

url_response = {
    "https://api.eloverblik.dk/customerapi/api/token": {
        "result": "test_token"
    },
    "https://api.eloverblik.dk/customerapi/api/meteringpoints/meteringpoints": {
        "result": [{'meteringPointId': 'value1'}, {'meteringPointId': 'value2'}]
    },
    "https://api.eloverblik.dk/customerapi/api/meterdata/gettimeseries/2023-01-01/2023-10-01/Actual": {
        "result": [
            {
            'mRID': 'test_mRID',
            'TimeSeries': [
                {
                    'mRID': 'test_mRID',
                'Period': [
                    {
                    'resolution': 'test_resolution',
                    'Point': [
                        {'position': '1'},
                    ]
                    }
                ]
                }
            ]
            }
        ]
    },
}



# custom class to be the mock return value of requests.get()
class MockGetResponse:

    def __init__(self, url) -> None:
        self.status_code = 200
        self.url = url

    def json(self):

        return url_response[self.url]

@pytest.fixture
def el_overblik():
    return ElOverblik(Secrets())


# monkeypatched requests.get moved to a fixture
@pytest.fixture
def mock_get_response(monkeypatch):
    """Requests.get() mocked to return MockGetResponse."""

    def mock_get(url, *args, **kwargs):
        return MockGetResponse(url)

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_get)


@pytest.fixture
def mock_get_too_many_calls_error_response(monkeypatch):
    """Requests.get() mocked to return MockGetResponse."""

    def mock_get(url, *args, **kwargs):
        response = MockGetResponse(url)
        response.status_code = 429
        return response

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_get)


@pytest.fixture
def mock_get_service_unavailable_error_response(monkeypatch):
    """Requests.get() mocked to return MockGetResponse."""
    def mock_get(url, *args, **kwargs):
        response = MockGetResponse(url)
        response.status_code = 503
        return response

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_get)



def test_get_token(mock_get_response, el_overblik):
    # Ensure that the token is retrieved successfully
    assert el_overblik.api_data_token == 'test_token'

def test_get_metering_points(mock_get_response, el_overblik):

    # Ensure that the metering points are retrieved successfully
    assert el_overblik.metering_points == ['value1', 'value2']

def test_too_many_calls_error_get_metering_points(mock_get_too_many_calls_error_response, el_overblik):
    # Ensure that the metering points are retrieved successfully
    with pytest.raises(ElOverblikTooManyRequestsException):
        el_overblik.metering_points

def test_service_unavailable_error_get_metering_points(mock_get_service_unavailable_error_response, el_overblik):
    # Ensure that the metering points are retrieved successfully
    with pytest.raises(ElOverblikServiceUnavailableException):
        el_overblik.metering_points

# Test get_time_series_data with successfull calls
def test_get_time_series_data(mock_get_response, el_overblik):
    # Ensure that the time series data is retrieved successfully
    assert el_overblik.get_timeseries_data(datetime.datetime(2023,1,1), datetime.datetime(2023,10,1)) == url_response['https://api.eloverblik.dk/customerapi/api/meterdata/gettimeseries/2023-01-01/2023-10-01/Actual']
