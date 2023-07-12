import logging
import os
import sys
import zipfile
import requests
import json
import pandas as pd
import wget

logger = logging.getLogger('inpidownloadlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def get_token():
    """fetch token to connect to api
    """
    endpoint = "https://registre-national-entreprises.inpi.fr/api/sso/login"

    body = json.dumps({"username": 'mail', "password":  'password'})

    response = requests.post(endpoint, data=body).json()
    if 'token' in response.keys() :
        return response['token']
    else :
        return 'Connexion error'
    

def download_inpi(path_root, sirenList):
    """Appelle successivement les entreprises recherchées sur l'API de l'INPI
    """

    nb_iter = int(len(sirenList)/100)+1

    for i in range(nb_iter):
    #for i in range(3):

        if (i%300==0):
            token = get_token()
            if token == "Erreur de connexion" :
                logging.info(token)
                return token
            else :
                logging.info('Connexion effectuée')

        logging.info(' ')
        logging.info(f'itereration : {i+1} / {nb_iter}')

        # seperate siren list into batchs of 100
        if i == int(len(sirenList)/100):
            sirenBatch = sirenList[100*i:]
        else : 
            sirenBatch = sirenList[100*i:100*(i+1)]
        sirenSearchBatch = ''.join(['&siren[]='+sirenNum for sirenNum in sirenBatch])

        endpoint = f"https://registre-national-entreprises.inpi.fr/api/companies?&pageSize=100{sirenSearchBatch}"
        headers = {"Authorization": "Bearer "+ token}

        try:     
            responseList = requests.get(endpoint, headers=headers).json()
            responseList = [item for sublist in responseList for item in sublist]
            inpi_df = pd.DataFrame.from_records(responseList)
            inpi_df.to_csv(os.path.join(path_root, 'inpi_df.csv'),
                           index=False,
                           sep=';')
        except Exception as e:
            logger.error('Récupération de la base inpi : %s', str(e))
