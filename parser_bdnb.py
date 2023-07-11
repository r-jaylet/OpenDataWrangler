"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (bdnb) de la base bdnb

    Documentation
    -------
        Description générale bdnb : https://www.data.gouv.fr/fr/datasets/base-bdnb-des-entreprises-et-de-leurs-etablissements-bdnb-siret/
        
"""
import logging
import sys

from utils.utils_bdnb import download_file_bdnb, extract_name_files_bdnb, extract_file_bdnb

logger = logging.getLogger('bdnblogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Bdnb:

    def __init__(self,
                 path_root: str = r".",
                 bdnb_file_name: str = r"open_data_millesime_2022-10-c_france_csv.tar\open_data_millesime_2022-10-c_france_csv.tar"):
        """constructor
        """
        self.path_root = path_root
        self.bdnb_file_name = bdnb_file_name

        # chargement initial des dictionnaires
        self.bdnb_file_df = self.load_bdnb_file_df()


    def load_bdnb(self):
        """Charge le context des données bdnb
        Parameters
        -------
        Returns
        -------
            df
        """
        logging.info("Process bdnb")
        try:
            bdnb_df = download_file_bdnb()
            if bdnb_df is not None:
                """
                tar_file_path = "../0_data/open_data_millesime_2022-10-c_france_csv.tar/open_data_millesime_2022-10-c_france_csv.tar"
                target_file = "./csv/proprietaire.csv"
                tar = tarfile.open(tar_file_path, "r")
                tar.extract(target_file)
                tar.close()
                """
                return bdnb_df.copy()
            
        except:
            return None


    def load_bdnb_file_df(self):
        """Charge le context des données bdnb
        Parameters
        -------
        Returns
        -------
            df
        """
        logging.info("Select bdnb file from :")
        logging.info(extract_name_files_bdnb(self.path_root))
        logging.info('Choose file name :')
        file_name = str(input())
        try:
            bdnb_file_df = extract_file_bdnb(self.path_root, file_name)
            return bdnb_file_df.copy()
        except:
            return None
        

    def get_bdnb_file_df(self):
        return self.bdnb_file_df
    

#bdnd = Bdnb(path_root=r"C:\Users\RémiJAYLET\Documents\City&You\0_data")