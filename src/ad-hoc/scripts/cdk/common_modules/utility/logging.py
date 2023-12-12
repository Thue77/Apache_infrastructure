import logging
from functools import wraps, partial
from pathlib import Path


# class Logger:
#     '''
#     Class to handle logging accross applikations and modules
#     '''

#     def __init__(self, name: str, format = '%(levelname)s - %(asctime)s - %(name)s - %(message)s') -> None:
#         self.logger = logging.getLogger(name)
#         self.logger.setLevel(logging.DEBUG)
#         # Remove all handlers associated with the logger object
#         self.logger.propagate = False

#         # Set format for logging
#         formatter = logging.Formatter(format)

#         # Set handler for logging
#         ch = logging.StreamHandler()
#         ch.setLevel(logging.DEBUG)

#         # Set formatter for handler
#         ch.setFormatter(formatter)

#         # Add handler to logger
#         self.logger.addHandler(ch)
    
#     def addLoggingLevel(self, levelName: str, levelNum: int, methodName=None) -> None:
#         """
#         Comprehensively adds a new logging level to the `logging` module and the
#         currently configured logging class.

#         `levelName` becomes an attribute of the `logging` module with the value
#         `levelNum`. `methodName` becomes a convenience method for both `logging`
#         itself and the class returned by `logging.getLoggerClass()` (usually just
#         `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
#         used.

#         To avoid accidental clobberings of existing attributes, this method will
#         raise an `AttributeError` if the level name is already an attribute of the
#         `logging` module or if the method name is already present 

#         Example
#         -------
#         >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
#         >>> logging.getLogger(__name__).setLevel("TRACE")
#         >>> logging.getLogger(__name__).trace('that worked')
#         >>> logging.trace('so did this')
#         >>> logging.TRACE
#         5

#         """
#         if not methodName:
#             methodName = levelName.lower()

#         if hasattr(logging, levelName):
#             raise AttributeError('{} already defined in logging module'.format(levelName))
#         if hasattr(logging, methodName):
#             raise AttributeError('{} already defined in logging module'.format(methodName))
#         if hasattr(logging.getLoggerClass(), methodName):
#             raise AttributeError('{} already defined in logger class'.format(methodName))

#         # This method was inspired by the answers to Stack Overflow post
#         # http://stackoverflow.com/q/2183233/2988730, especially
#         # http://stackoverflow.com/a/13638084/2988730
#         def logForLevel(self, message, *args, **kwargs):
#             if self.isEnabledFor(levelNum):
#                 self._log(levelNum, message, args, **kwargs)
#         def logToRoot(message, *args, **kwargs):
#             logging.log(levelNum, message, *args, **kwargs)

#         logging.addLevelName(levelNum, levelName)
#         setattr(logging, levelName, levelNum)
#         setattr(logging.getLoggerClass(), methodName, logForLevel)
#         setattr(logging, methodName, logToRoot)

#     def get_logger(self) -> logging.Logger:
#         return self.logger
    

# # Set logging
# ModuleLog = Logger(Path(__file__).stem)

# ModuleLog.addLoggingLevel('MODULEINFO', logging.INFO - 5)

# module_logger = ModuleLog.get_logger()


# def module_logging(func):

#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         module_logger.moduleinfo(f"Running {func.__name__}")
#         return func(*args, **kwargs)
#     return wrapper


import logging.config
from pythonjsonlogger import jsonlogger

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        }
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "json",
        }
    },
    "loggers": {"": {"handlers": ["stdout"], "level": "INFO"}},
}


logging.config.dictConfig(LOGGING)


def module_logging(func, logger: logging.Logger):

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Running {func.__name__} with args: {args} and kwargs: {kwargs}")
        value = func(*args, **kwargs)
        logger.info(f"Finished running {func.__name__}")
        return value
    return wrapper

logger = logging.getLogger(__name__)

module_logger = partial(module_logging, logger=logger)