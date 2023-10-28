import requests
from typing import Protocol
from retrying import retry

# Interface for secrets module
class SecretStore(Protocol):
    def get_secret(name: str):
        pass


# Custom exceptions for ElOverblik
class ElOverblikException(Exception):
    pass

class ElOverblikTooManyRequestsException(Exception):
    '''
    Error raised when ElOverblik returns 429 status code
    '''
    pass

class ElOverblikServiceUnavailableException(Exception):
    '''
    Error raised when ElOverblik returns 503 status code
    '''
    pass

def retry_error(exception):
    """Return True if we should retry. Retries on 429 and 503 errors"""
    return isinstance(exception, ElOverblikTooManyRequestsException) or isinstance(exception, ElOverblikServiceUnavailableException)


class ElOverblik:
    '''
    Class for interacting with ElOverblik API. For API documentation please refer to https://api.eloverblik.dk/customerapi/index.html
    '''
    def __init__(self, secret_store: SecretStore):
        self.url = f"https://api.eloverblik.dk/customerapi"
        self.secret_store = secret_store
        self.api_data_token = self.__get_data_token()

    def __get_data_token(self):
        '''
        Gets data token from ElOverblik API. The data token is used to authenticate requests to the API. The data token is valid for 24 hours.
        '''
        api_token = self.secret_store.get_secret("el_overblik_api_token")
        api_endpoint = '/api/token'
        headers = {
            "Authorization": f"Bearer {api_token}"
        }
        response = requests.get(self.url + api_endpoint, headers=headers)
        return response.json()["result"]
    
    
    @retry(retry_on_exception=retry_error, stop_max_attempt_number=3, wait_exponential_multiplier=6000, wait_exponential_max=300000)
    def get_timeseries_data(self, start, end, aggregate="Actual"):
        '''
        Get time series data from ElOverblik for all available meters

        Parameters:
        start (str): Start date in format "YYYY-MM-DD"
        end (str): End date in format "YYYY-MM-DD"
        aggregate (str): Aggregate data by "Quarter", "Hour", "Day", "Month", "Year", or "Actual" (default: "Actual")
        '''
        api_endpoint = '/api/meterdata/gettimeseries'
        params = {
            "dateFrom": start,
            "dateTo": end,
            "aggregation": aggregate
        }
        metering_point = ["571313155411222551"]
        body = {
                "meteringPoints": {
                    "meteringPoint": metering_point
                }
                }
        url = self.url + api_endpoint + '/' + '/'.join([params["dateFrom"], params["dateTo"], params["aggregation"]])
        response = requests.post(url, json=body, headers={"Authorization": f"Bearer {self.api_data_token}"})
        if response.status_code == 429:
            raise ElOverblikTooManyRequestsException("Too many requests to ElOverblik API")
        elif response.status_code == 503:
            raise ElOverblikServiceUnavailableException("ElOverblik API is unavailable")
        elif response.status_code != 200:
            raise ElOverblikException(f"Error getting time series data. Status code: {response.status_code}")

        return response.json()
    


if __name__ == "__main__":

    from cdk.common_modules.access.secrets import Secrets
    el_overblik = ElOverblik(Secrets())

    data = el_overblik.get_timeseries_data("2023-09-01", "2023-09-02")

    print(data)