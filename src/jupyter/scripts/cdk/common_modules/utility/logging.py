import logging

class Logger:
    '''
    Class to handle logging accross applikations and modules
    '''

    def __init__(self, name: str, format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s') -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        # Remove all handlers associated with the logger object
        self.logger.propagate = False

        # Set format for logging
        formatter = logging.Formatter(format)

        # Set handler for logging
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # Set formatter for handler
        ch.setFormatter(formatter)

        # Add handler to logger
        self.logger.addHandler(ch)

    def get_logger(self):
        return self.logger