import logging
import os
import sys
import zipfile

import pandas as pd
import wget

logger = logging.getLogger('coprodownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_copro():
    """download associated file 
    """

    try:
        copro_df = None
        return copro_df
    
    except Exception as e:
        logger.error('Récupération de la base copro : %s', str(e))
        return None
