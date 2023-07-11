import logging
import os
import sys
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

logger = logging.getLogger('bodaccdownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def download_file_bodacc():
    """download associated file 
    """

    try:

        PATH = r'‪C:\chromedriver.exe'
        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        # select filter parameters
        publicationavis= 'A'
        familleavis_lib= 'Proc%C3%A9dures+collectives'
        typeavis= 'Avis+initial'

        # connect to url to download page
        url = f'https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/export/?sort=dateparution&rows=10000&refine.publicationavis={publicationavis}&refine.familleavis_lib={familleavis_lib}&refine.typeavis_lib={typeavis}'
        driver.get(url)

        # click on download button
        try:
            driver.find_element(By.XPATH,'//*[@id="main"]/div/div[4]/div[2]/div[2]/div[8]/div/div/div/div[1]/ul/li[1]/div/a[2]/i').click()
        except:
            pass






        
        bodacc_df = None
        return bodacc_df
    
    except Exception as e:
        logger.error('Récupération de la base bodacc : %s', str(e))
        return None
