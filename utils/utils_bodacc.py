import logging
import os
import sys
import time
import requests
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger('bodaccdownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_bodacc_export(path_root,
                                publicationavis='A',
                                typeavis='',
                                familleavis='',
                                numerodepartement=''):
    """Download all rows of bodacc data from bodacc export 
    """

    try:
        # initialize webdriver
        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        # connect to url to download page
        url = f'https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/export/?sort=dateparution&refine.publicationavis={publicationavis}'
        if typeavis != '':
            url = url + f'&refine.typeavis={typeavis}'
        if familleavis != '':
            url = url + f'&refine.familleavis={familleavis}'
        if numerodepartement != '':
            url = url + f'&refine.numerodepartement={numerodepartement}'

        # click on download button
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(url)
        driver.find_element(
            By.XPATH, '//*[@id="main"]/div/div[4]/div[2]/div[2]/div[8]/div/div/div/div[1]/ul/li[1]/div/a[2]/i').click()
        logger.info('Downloading Bodacc...')

        # waiting for end of download
        while 'annonces-commerciales.csv' not in os.listdir(os.path.expanduser("~\Downloads")):
            time.sleep(10)
            logger.info('Still waiting...')

        # save file
        path_source = os.path.expanduser("~/Downloads/annonces-commerciales.csv")
        path_destination = os.path.expanduser(path_root)
        filename_csv = f'annonces-commerciales_{publicationavis}_{typeavis}_{familleavis}_{numerodepartement}_export.csv'
        if filename_csv not in os.listdir(path_destination):
            file_destination = os.path.join(path_destination, filename_csv)
            os.rename(path_source, file_destination)
        else:
            logger.info('file already exists')

        # close webdriver
        driver.close()

    except Exception as e:
        logger.error('Bodacc data fetching : %s', str(e))


def download_file_bodacc_api(path_root,
                             publicationavis='A',
                             typeavis='',
                             familleavis='',
                             numerodepartement='',
                             nrows = '10000'):    
    """Download last 10000 rows of bodacc data from bodacc api
    """
    try:
        # connect API v1.0 BODACC
        url = f'https://bodacc-datadila.opendatasoft.com/api/records/1.0/search/?dataset=annonces-commerciales&q=&rows={nrows}&sort=dateparution&facet=publicationavis&facet=publicationavis_facette&facet=typeavis&facet=typeavis_lib&facet=familleavis&facet=familleavis_lib&facet=numerodepartement&facet=departement_nom_officiel'
        if typeavis != '':
            url = url + f'&refine.publicationavis={publicationavis}'
        if familleavis != '':
            url = url + f'&refine.familleavis={familleavis}'
        if numerodepartement != '':
            url = url + f'&refine.numerodepartement={numerodepartement}'

        # stock in dataframe
        response = requests.get(url)
        bodacc_api = pd.DataFrame.from_records(response.json()['records'])
        bodacc_api.to_csv(os.path.join(path_root, f'annonces-commerciales_{publicationavis}_{typeavis}_{familleavis}_{numerodepartement}_api.csv'), index=False)
        
    except Exception as e:
        logger.error('Bodacc data fetching : %s', str(e))
