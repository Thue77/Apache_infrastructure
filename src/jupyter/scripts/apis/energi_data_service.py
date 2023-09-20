import requests


class EnergiDataService:
    def __init__(self, dataset):
        self.url = f"https://api.energidataservice.dk/dataset/{dataset}"
        self.dataset = dataset
    
    def get_data(self, start, end):
        params = {
            "offset": 0,
            "start": start,
            "end": end,
            "timezone": "dk"
        }
        response = requests.get(self.url, params=params)
        return response.json()


if __name__ == "__main__":
    energi_data_service = EnergiDataService("ConsumptionDK3619codehour")

    data = energi_data_service.get_data("2023-09-01T00:00", "2023-09-02T00:00")

    print(data)