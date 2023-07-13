"""OpenDataWrapper

    Summary
    -------
        Manipulation des données sur les entrperises de la base INPI

    Documentation
    -------
        Description générale INPI : https://data.inpi.fr/content/editorial/Acces_API_Entreprises

    Packages
    -------
        utils_inpi
"""
import logging
import os
import sys

import pandas as pd

from utils.utils_inpi import (download_inpi_list)

logger = logging.getLogger('inpilogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Inpi:

    def __init__(self,
                 path_root: str = r".",
                 sirenList: list = None):
        """constructor
        """
        self.path_root = path_root
        self.sirenList = sirenList

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
            print("a")
            inpi_df = download_inpi_list(self.sirenList)
            print("b")
            if inpi_df is not None:
                print("c")
                inpi_normalized_df = pd.json_normalize(inpi_df['formality'])
                inpi_df = pd.concat(
                    [inpi_df.drop('formality', axis=1),
                     inpi_normalized_df.drop('siren', axis=1)],
                    axis=1)

                inpi_df.to_csv(os.path.join(self.path_root, 'inpi_df.csv'), index=False, sep=';')

                return inpi_df.copy()

        except Exception as e:
            logging.error('Erreur chargement base : %s', str(e))

    def get_inpi_df(self):
        return self.inpi_df
