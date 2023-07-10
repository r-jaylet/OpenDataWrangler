import logging
import os
import sys
import zipfile

import wget

logger = logging.getLogger('sirendownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_sirene(path_root, filename_sirene):
    """download associated file 
    """
    try:
        # download zip file
        url = f'https://files.data.gouv.fr/insee-sirene/Stock{filename_sirene}_utf8.zip'
        filename_zip = f'Stock{filename_sirene}_utf8.zip'
        filename_csv = f'Stock{filename_sirene}_utf8.csv'
        wget.download(url, os.path.join(path_root, filename_zip))

        # extract csv file
        with zipfile.ZipFile(os.path.join(path_root, filename_zip), 'r') as zip:
            zip.extract(filename_csv, path_root)
        os.remove(os.path.join(path_root, filename_zip))

    except Exception as e:
        logger.error('Récupération de la base siren : %s', str(e))
