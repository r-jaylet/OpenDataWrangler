
import logging
import os
import sys
import zipfile
import tarfile

import pandas as pd
import wget

logger = logging.getLogger('bdnbdownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_bdnb():
    """download associated file 
    """
    try:
        url = 'https://www.data.gouv.fr/fr/datasets/r/ad4bb2f6-0f40-46d2-a636-8d2604532f74'
        #download tar.gz file from url
        filename_tar = 'open_data_millesime_2022-10-c_france_csv.tar'
        df_bdnb = None
        return df_bdnb
    
    except Exception as e:
        return None
    
    
def extract_name_files_bdnb(path_root):
    """extract name files from tar.gz file
    """
    try:
        filename_tar = r"open_data_millesime_2022-10-c_france_csv.tar\open_data_millesime_2022-10-c_france_csv.tar"
        tar_file_path = os.path.join(path_root, filename_tar)
        tar = tarfile.open(tar_file_path, "r")
        tar_names = tar.getnames()

        # transform in list of csv file names
        file_names = []
        for element in tar_names:
            if str(element)[-4:] == '.csv':
                file_names.append(element.replace("./csv/", ""))
        tar.close()
        return file_names
    
    except Exception as e:
        logging.error('Récupération de la base bdnb : %s', str(e))
        return None
    

def extract_file_bdnb(path_root, file_name):
    """extract file from tar.gz file
    """
    try:
        filename_tar = r"open_data_millesime_2022-10-c_france_csv.tar\open_data_millesime_2022-10-c_france_csv.tar"
        tar_file_path = os.path.join(path_root, filename_tar)
        tar = tarfile.open(tar_file_path, "r")
        
        target_file = "./csv/" + file_name

        tar.extract(target_file, path=path_root)
        tar.close()
        csv_file_path = os.path.join(path_root, 'csv/' + file_name)
        return pd.read_csv(csv_file_path, nrows=5)
    
    except Exception as e:
        logging.error('Récupération de la base bdnb : %s', str(e))
        return None