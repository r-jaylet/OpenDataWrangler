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


def download_file_majic_locaux(path_root, year):
    """download associated file 
    """

    try:
        # download zip file
        url = f'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales/attachments/fichier_des_locaux_situation_{year}_zip'
        filename_zip = f'fichier_des_locaux_situation_{year}_zip'

        wget.download(url, os.path.join(path_root, filename_zip))

    except Exception as e:
        logger.error('Récupération de la base locaux : %s', str(e))
        return None


def download_file_majic_parcelles(path_root, year):
    """download associated file 
    """

    try:
        # download zip file
        url1 = f'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales/attachments/fichier_des_parcelles_situation_{year}_dept_01_a_61_zip'
        url2 = f'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales/attachments/fichier_des_parcelles_situation_{year}_dept_62_a_976_zip'
        filename_zip1 = f'fichier_des_parcelles_situation_{year}_dept_01_a_61_zip'
        filename_zip2 = f'fichier_des_parcelles_situation_{year}_dept_62_a_976_zip'
        wget.download(url1, os.path.join(path_root, filename_zip1))
        wget.download(url2, os.path.join(path_root, filename_zip2))

    except Exception as e:
        logger.error('Récupération de la base parcelles : %s', str(e))
        return None
