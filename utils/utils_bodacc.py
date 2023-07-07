import logging
import os
import sys
import zipfile

import pandas as pd
import wget

logger = logging.getLogger('bodaccdownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_bodacc():
    """download associated file 
    """

    try:
        bodacc_df = None
        return bodacc_df
    
    except Exception as e:
        logger.error('Récupération de la base bodacc : %s', str(e))
        return None
