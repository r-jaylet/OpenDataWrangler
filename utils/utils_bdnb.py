
import logging
import os
import sys
import zipfile

import pandas as pd
import wget

logger = logging.getLogger('bdnbdownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_bdnb():
    """download associated file 
    """
    try:
        url = 'https://www.data.gouv.fr/fr/datasets/r/ad4bb2f6-0f40-46d2-a636-8d2604532f74'
        #download tar.gz file from url
        filename_tar = 'open_data_millesime_2022-10-c_france_csv.tar'
        df_bdnb = None
        return df_bdnb
    
    except Exception as e:
        return None
    


