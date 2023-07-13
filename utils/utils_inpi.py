import logging
import sys
import requests
import json
import pandas as pd

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
    body = json.dumps({"username": 'inpi.onze@gmail.com', "password":  'inpiOnze11!!'})
    response = requests.post(endpoint, data=body).json()

    if 'token' in response.keys() :
        return response['token']
    else :
        return 'Connexion error'
    

def download_inpi_list(sirenList):
    """Appelle successivement les entreprises recherchées sur l'API de l'INPI
    """
    resultResponseList = []
    batch_size = 100
    nb_iter = (len(sirenList) + batch_size - 1) // batch_size

    try:
        for i in range(nb_iter):    
            if (i%300==0):
                token = get_token()
                if token == "Erreur de connexion" :
                    logging.info(token)
                else :
                    logging.info('Connexion effectuée')

            logging.info(' ')
            logging.info(f'itereration : {i+1} / {nb_iter}')

            # seperate siren list into batchs of 100
            start_index = i * batch_size
            end_index = min((i+1) * batch_size, len(sirenList))
            sirenBatch = sirenList[start_index:end_index]
            sirenSearchBatch = ''.join(['&siren[]=' + sirenNum for sirenNum in sirenBatch])

            endpoint = f"https://registre-national-entreprises.inpi.fr/api/companies?&pageSize=100{sirenSearchBatch}"
            headers = {"Authorization": "Bearer "+ token}
            responseList = requests.get(endpoint, headers=headers).json()
            resultResponseList.append(responseList)

        resultResponseList = [item for sublist in resultResponseList for item in sublist]
        inpi_df = pd.DataFrame.from_records(resultResponseList)
            
        return inpi_df
            
    except Exception as e:
        logger.error('Récupération de la base inpi : %s', str(e))
        return None


