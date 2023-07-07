"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (bodacc) de la base bodacc

    Documentation
    -------
        Description générale bodacc : https://www.data.gouv.fr/fr/datasets/base-bodacc-des-entreprises-et-de-leurs-etablissements-bodacc-siret/
        
"""
import logging
import sys

import pandas as pd

from utils.utils_bodacc import download_file_bodacc

logger = logging.getLogger('bodacclogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class bodacc:

    def __init__(self):
        """constructor
        """

        # chargement initial des dictionnaires
        self.bodacc_df = self.load_bodacc_df()

    def load_bodacc_df(self):
        """Charge le context des données bodacc
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process bodacc")

        try:
            bodacc_df = download_file_bodacc()

            if bodacc_df is not None:

                bodacc_df = None

                return bodacc_df.copy()
            
        except:
            return None

    def get_bodacc_df(self):
        return self.bodacc_df
