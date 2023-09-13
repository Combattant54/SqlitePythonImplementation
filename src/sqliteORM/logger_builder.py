import logging
import os


DIR_PATH = os.getcwd()
SPAM_PATH = os.path.join(DIR_PATH, "spam.log")
FILE_PATH = os.path.join(DIR_PATH, "logs.log")

print(DIR_PATH, SPAM_PATH, FILE_PATH)

# cré les fichier s'ils n'existent pas
if not os.path.exists(SPAM_PATH):
    with open(SPAM_PATH, "a"):
        pass
if not os.path.exists(FILE_PATH):
    with open(FILE_PATH, "a"):
        pass


# cré le formatter du modul de log
formatter = logging.Formatter("%(levelname)s - %(name)s - %(lineno)s - %(asctime)s - %(message)s")


# cré le handler pour le fichier de spam qui contient tt les messages
spam_handler = logging.FileHandler(SPAM_PATH, mode="w")
spam_handler.setLevel(logging.DEBUG)
spam_handler.setFormatter(formatter)

# cré le handler pour le fichier de log qui contient les messages d'erreurs et de warning
file_handler = logging.FileHandler(FILE_PATH, mode="w")
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(formatter)

# cré le handler pour le stream de la console pour les erreurs majeures
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.ERROR)
stream_handler.setFormatter(formatter)

LOGGER_LEVEL = logging.DEBUG
LOGGERS = []

# fonction pour créer un logger spéciale commun a tout le projet
def build_logger(name) -> logging.Logger:
    new_logger = logging.getLogger(name)
    new_logger.setLevel(LOGGER_LEVEL)
    new_logger.addHandler(spam_handler)
    new_logger.addHandler(file_handler)
    new_logger.addHandler(stream_handler)
    
    LOGGERS.append(new_logger)
    
    return new_logger

def set_level(level):
    global LOGGER_LEVEL
    LOGGER_LEVEL = level
    
    for logger in LOGGERS:
        logger.setLevel(level)