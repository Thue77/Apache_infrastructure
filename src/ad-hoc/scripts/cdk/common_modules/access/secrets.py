from decouple import config
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

class Secret:
    '''Class to represent secrets
    '''
    
    def __init__(self, s) -> None:
        self.s = s
    
    def __str__(self) -> str:
        return 'REDACTED'
    
    def __repr__(self) -> str:
        return 'REDACTED'

class Secrets:
    '''Class to represent secrets. Secrets are stored in .env or .ini files in the root of the project.
    Alternatively they can be stored in environment variables.
    '''
    
    def __init__(self) -> None:
        pass
    
    def get_secret(self, secret_name):
        logging.info(f"Loading secret {secret_name}")
        return config(secret_name)