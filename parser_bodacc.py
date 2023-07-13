"""OpenDataWrapper

    Summary
    -------
        Manipulation des données sur les annonces commerciales de la base BODACC (API)

    Documentation
    -------
        Description générale BODACC : https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/api/?sort=dateparution

    Packages
    -------
        utils_bodacc
"""
import logging
import sys

from utils.utils_bodacc import (download_file_bodacc_api, download_file_bodacc_export)

logger = logging.getLogger('bodacclogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Bodacc:

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
            # bodacc_df = utils_bodacc.download_file_bodacc()
            bodacc_df = None
            if bodacc_df is not None:

                bodacc_df = None

                return bodacc_df.copy()

        except:
            return None

    def get_bodacc_df(self):
        return self.bodacc_df
