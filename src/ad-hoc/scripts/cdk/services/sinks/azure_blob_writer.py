from azure.storage.blob import BlobServiceClient

from cdk.common_modules.access.secrets import Secrets
from cdk.common_modules.models.file import File


class AzureBlobWriter:
    def __init__(self, storage_account_name, container_name, secret_store: Secrets, type='standard'):
        self.secret_store = secret_store
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.type = type

    @property
    def access_key(self):
        return self.secret_store.get_secret(f'ADLS_{self.storage_account_name}_access_key')
    
    @property
    def connection_string(self):
        # Return Azure blob storage connection string based on access key
        return f'DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.access_key};EndpointSuffix=core.windows.net'

    def upload_file(self, data, file: File) -> None:
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=file.path + file.name + '.json')
        blob_client.upload_blob(str(data), overwrite=True)

    def write_data(self, data, file: File) -> None:
        if self.type == 'standard':
            self.upload_file(data, file)
        else:
            raise ValueError(f'Unknown type {self.type}')