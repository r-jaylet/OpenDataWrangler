import logging
import os
import sys
import zipfile

import pandas as pd
import wget

logger = logging.getLogger('inpidownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_inpi():
    """download associated file 
    """

    try:
        inpi_df = None
        return inpi_df
    
    except Exception as e:
        logger.error('Récupération de la base inpi : %s', str(e))
        return None
