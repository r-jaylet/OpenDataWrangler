import logging
import os
import requests
import sys

import pandas as pd
import wget

logger = logging.getLogger('coprodownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_copro(path_root):
    """download associated file 
    """

    try:
        # download csv file
        url = "https://static.data.gouv.fr/resources/registre-national-dimmatriculation-des-coproprietes/20230703-161412/rnc-data-gouv-with-qpv.csv"
        filename = "rnc-data-gouv-with-qpv.csv"

        response = requests.get(url)
        response.raise_for_status()

        file_path = os.path.join(path_root, filename)
        with open(file_path, "wb") as file:
            file.write(response.content)

    except Exception as e:
        logging.error('Récupération de la base copro : %s', str(e))
        return None
