"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (inpi) de la base inpi

    Documentation
    -------
        Description générale inpi : https://www.data.gouv.fr/fr/datasets/base-inpi-des-entreprises-et-de-leurs-etablissements-inpi-siret/
        
"""
import logging
import sys

import pandas as pd

from old_utils.utils_inpi import download_file_inpi

logger = logging.getLogger('inpilogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class inpi:

    def __init__(self):
        """constructor
        """

        # chargement initial des dictionnaires
        self.inpi_df = self.load_inpi_df()

    def load_inpi_df(self):
        """Charge le context des données inpi
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process inpi")

        try:
            inpi_df = download_file_inpi()

            if inpi_df is not None:

                inpi_df = None

                return inpi_df.copy()
            
        except:
            return None

    def get_inpi_df(self):
        return self.inpi_df
