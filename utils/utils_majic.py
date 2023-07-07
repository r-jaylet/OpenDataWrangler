import logging
import os
import sys
import zipfile

import pandas as pd
import wget

logger = logging.getLogger('majicdownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_majic():
    """download associated file 
    """

    try:
        majic_df = None
        return majic_df
    
    except Exception as e:
        logger.error('Récupération de la base majic : %s', str(e))
        return None
