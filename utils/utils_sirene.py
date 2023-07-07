import logging
import os
import sys
import zipfile

import pandas as pd
import wget

logger = logging.getLogger('sirendownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_sirene(filename_sirene):
    """download associated file 
    """
    try:
        # download zip file
        url = f'https://files.data.gouv.fr/insee-sirene/Stock{filename_sirene}_utf8.zip'
        filename_zip = f'Stock{filename_sirene}_utf8.zip'
        filename_csv = f'Stock{filename_sirene}_utf8.csv'
        wget.download(url, filename_zip)

        # extract csv file
        with zipfile.ZipFile(filename_zip, 'r') as zip:
            zip.extract(filename_csv)
        os.remove(filename_zip)
    
    except Exception as e:
        logger.error('Récupération de la base siren : %s', str(e))
