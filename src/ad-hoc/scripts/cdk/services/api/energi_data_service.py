import requests

# Custom exception for EnergiDataService
class EnergiDataServiceException(Exception):
    pass

class EnergiDataService:
    def __init__(self, dataset):
        self.url = f"https://api.energidataservice.dk/dataset/{dataset}"
        self.dataset = dataset
        self.data_field = "records"
    
    def get_data(self, start, end, raw = False):
        params = {
            "offset": 0,
            "start": start,
            "end": end,
            "timezone": "dk"
        }
        response = requests.get(self.url, params=params)
        if response.status_code != 200:
            raise EnergiDataServiceException(f"Error getting data from {self.dataset}. Status code: {response.status_code}")
        if raw:
            return response.json()
        return response.json()[self.data_field]


if __name__ == "__main__":
    energi_data_service = EnergiDataService("ConsumptionDK3619codehour")

    data = energi_data_service.get_data("2023-09-01T00:00", "2023-09-02T00:00")

    # print(data)