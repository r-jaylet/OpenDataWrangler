"""OpenDataWrapper

    Summary
    -------
        Manipulation des données sur les copropriétés de la base RNIC

    Documentation
    -------
        Description générale RNIC : https://www.data.gouv.fr/fr/datasets/registre-national-dimmatriculation-des-coproprietes/)https://www.data.gouv.fr/fr/datasets/registre-national-dimmatriculation-des-coproprietes/

    Packages
    -------
        utils_copro
"""
import logging
import os
import sys

import pandas as pd

from utils.utils_copro import (download_file_copro)

logger = logging.getLogger('coprologging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Copro:

    def __init__(self,
                 path_root: str = r".",
                 file_name: str = "rnc-data-gouv-with-qpv.csv"):
        """constructor
        """
        self.path_root = path_root
        self.file_name = file_name

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
            # checks if document is present in file. If not, triggers the download function
            try:
                if self.file_name not in os.listdir(self.path_root):
                    logging.info("Download Copro")
                    download_file_copro(self.path_root)
                else:
                    logging.info("Copro already downloaded")
            except:
                logging.info("Download Copro")
                download_file_copro(self.path_root)

            logging.info("Upload Copro")
            path_file = os.path.join(self.path_root, self.file_name)
            copro_df = pd.read_csv(path_file,
                                   dtype={'Siret représentant légal (si existe)': str},
                                   sep=',', nrows=10000)

            if copro_df is not None:
                logging.info("Processing Copro")
                copro_df = copro_df.copy()

                return copro_df.copy()

        except Exception as e:
            logging.error('Erreur chargement base : %s', str(e))

    def get_copro_df(self):
        return self.copro_df
