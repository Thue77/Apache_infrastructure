from typing import Any
import pytest
import requests

from cdk.services.api.el_overblik import ElOverblik, ElOverblikException, ElOverblikTooManyRequestsException, ElOverblikServiceUnavailableException
from cdk.common_modules.access.secrets import Secrets

get_url_response = {
    "https://api.eloverblik.dk/customerapi/api/token": {
        "result": "test_token"
    },
    "https://api.eloverblik.dk/customerapi/api/meteringpoints/meteringpoints": {
        "result": [{'meteringPointId': 'value1'}, {'meteringPointId': 'value2'}]
    },
    "https://api.eloverblik.dk/customerapi/api/meterdata/gettimeseries/": {
        "result": {'metering_data': 'test_data'}
    },
}

post_url_response = {
    "https://api.eloverblik.dk/customerapi/api/meterdata/gettimeseries/": {
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

        return get_url_response[self.url]


# monkeypatched requests.get moved to a fixture
@pytest.fixture
def mock_get_response(monkeypatch):
    """Requests.get() mocked to return MockGetResponse."""

    def mock_get(url, *args, **kwargs):
        return MockGetResponse(url)

    monkeypatch.setattr(requests, "get", mock_get)

@pytest.fixture
def el_overblik():
    return ElOverblik(Secrets())

@pytest.fixture
def mock_error_response(monkeypatch):
    """Requests.get() mocked to return MockGetResponse."""

    def mock_get(url, *args, **kwargs):
        response = MockGetResponse(url)
        response.status_code = 429
        return response

    monkeypatch.setattr(requests, "get", mock_get)


def test_get_token(mock_get_response, el_overblik):

    # Ensure that the token is retrieved successfully
    assert el_overblik.api_data_token == 'test_token'

def test_get_metering_points(mock_get_response, el_overblik):

    # Ensure that the metering points are retrieved successfully
    assert el_overblik.metering_points == ['value1', 'value2']

def test_too_many_calls_error_get_metering_points(mock_error_response, el_overblik):
    # Ensure that the metering points are retrieved successfully
    with pytest.raises(ElOverblikTooManyRequestsException):
        el_overblik.metering_points
