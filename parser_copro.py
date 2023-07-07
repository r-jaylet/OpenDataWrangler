"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (copro) de la base copro

    Documentation
    -------
        Description générale copro : https://www.data.gouv.fr/fr/datasets/base-copro-des-entreprises-et-de-leurs-etablissements-copro-siret/
        
"""
import logging
import sys

import pandas as pd

from utils.utils_copro import download_file_copro

logger = logging.getLogger('coprologging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class copro:

    def __init__(self):
        """constructor
        """

        # chargement initial des dictionnaires
        self.copro_df = self.load_copro_df()

    def load_copro_df(self):
        """Charge le context des données copro
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process copro")

        try:
            copro_df = download_file_copro()

            if copro_df is not None:

                copro_df = None

                return copro_df.copy()
            
        except:
            return None

    def get_copro_df(self):
        return self.copro_df
