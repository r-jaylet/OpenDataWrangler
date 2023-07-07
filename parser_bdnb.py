"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (BDNB) de la base bdnb

    Documentation
    -------
        Description générale bdnb : https://www.data.gouv.fr/fr/datasets/base-bdnb-des-entreprises-et-de-leurs-etablissements-BDNB-siret/
        
"""
import logging
import sys
import tarfile

import pandas as pd

from utils.utils_bdnb import download_file_bdnb

logger = logging.getLogger('BDNBlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class BDDB:

    def __init__(self):
        """constructor
        """

        # chargement initial des dictionnaires
        self.BDNB_df = self.load_BDNB_df()

    def load_BDNB_df(self):
        """Charge le context des données BDNB
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process BDNB")

        try:
            BDNB_df = download_file_bdnb()

            if BDNB_df is not None:
                """
                tar_file_path = "../0_data/open_data_millesime_2022-10-c_france_csv.tar/open_data_millesime_2022-10-c_france_csv.tar"
                target_file = "./csv/proprietaire.csv"
                tar = tarfile.open(tar_file_path, "r")
                tar.extract(target_file)
                tar.close()
                """
                return BDNB_df.copy()
            
        except:
            return None
        
    def get_siret_df(self):
        return self.siret_df
