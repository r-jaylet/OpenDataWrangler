"""City & You Open Data Use Case Exploration

    Summary
    -------
        Récupération des données sur les représentants d'entreprises de l'API de l'INPI

    Documentation
    -------
        Description générale API INPI : https://www.inpi.fr/sites/default/files/documentation%20technique%20API%20formalit%C3%A9s_v13.pdf
        
"""
import json
import logging
import sys
import time

import numpy as np
import requests

logger = logging.getLogger('inpicrawlerlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


def get_token(userId):
    """reccupère un token pour se connecter à l'API de l'INPI
    """
    endpoint = "https://registre-national-entreprises.inpi.fr/api/sso/login"

    # récupère code d'identification des users créés
    with open('accounts_inpi.json') as file:
        userJson = json.load(file)
    userList = userJson['accounts']

    userId = userId%(len(userList)) # prend élément dans liste des users en fonction de iteration dans userList
    userSelected = userList[userId]
    mail = userSelected['mail']
    password = userSelected['inpiPassword']
    body = json.dumps({"username": mail, "password":  password})

    try:
        response = requests.post(endpoint, data=body).json()
        if 'token' in response.keys():
            return response['token']
        else:
            return "Erreur de connexion"
    except:
        #  Réessaye deuxième fois si erreur de connexion
        try:
            response = requests.post(endpoint, data=body).json()
            if 'token' in response.keys():
                return response['token']
        except:
                return "Erreur de connexion"
    

def process_response(responseList, result):
    """sélectionne les variables pertinentes dans les appels à l'API
    """
    resultResponseList = []

    if type(responseList) == dict:
        logging.info("Erreur : données pas dans le bon format")
        return resultResponseList, result

    else:
        for response in responseList:
            resultResponse = {}

            resultResponse['siren'] = response['siren']
            resultResponse['updatedAt'] = response['updatedAt']

            # recupère la composition des représentants des personnes personnes morales
            resultResponsePouvoir = []
            if 'formality' in response and 'content' in response['formality'] and 'personneMorale' in response['formality']['content'] and 'composition' in response['formality']['content']['personneMorale']:
                responsePouvoirs = response['formality']['content']['personneMorale']['composition']['pouvoirs']
                for entite in responsePouvoirs:
                    resultEntite = {}

                    # représentant est un individu
                    if 'individu' in entite:
                        resultEntite['typePouvoir'] = 'individu'
                        entiteDescription = entite['individu']['descriptionPersonne']
                        resultEntite['individuDateDeNaissance'] = entiteDescription['dateDeNaissance'] if 'dateDeNaissance' in entiteDescription else np.nan
                        resultEntite['individuNom'] = entiteDescription['nom'] if 'nom' in entiteDescription else np.nan
                        resultEntite['individuPrenoms'] = entiteDescription['prenoms'] if 'prenoms' in entiteDescription else np.nan
                        resultEntite['individuNomUsage'] = entiteDescription['nomUsage'] if 'nomUsage' in entiteDescription else np.nan
                        resultEntite['individuRole'] = entiteDescription['role'] if 'role' in entiteDescription else np.nan
                        resultEntite['individuDateEffetRoleDeclarant'] = entiteDescription['dateEffetRoleDeclarant'] if 'dateEffetRoleDeclarant' in entiteDescription else np.nan
                        resultEntite['individuNationalite'] = entiteDescription['nationalite'] if 'nationalite' in entiteDescription else np.nan
                        resultEntite['individuSituationMatrimoniale'] = entiteDescription['situationMatrimoniale'] if 'situationMatrimoniale' in entiteDescription else np.nan

                    # représentant est une entreprise
                    if 'entreprise' in entite:
                        resultEntite['typePouvoir'] = 'entreprise'
                        entiteDescription = entite['entreprise']
                        resultEntite['entrepriseRole'] = entiteDescription['roleEntreprise'] if 'roleEntreprise' in entiteDescription else np.nan
                        resultEntite['entrepriseSiren'] = entiteDescription['siren'] if 'siren' in entiteDescription else np.nan
                        resultEntite['entrepriseDenomination'] = entiteDescription['denomination'] if 'denomination' in entiteDescription else np.nan
                        
                    resultResponsePouvoir.append(resultEntite)
            resultResponse['pouvoirs'] = resultResponsePouvoir

            resultResponseList.append(resultResponse)
        result.append(resultResponseList)

        return resultResponseList, result


def inpiCrawler(sirenList, userId):
    """Appelle successivement les entreprises recherchées sur l'API de l'INPI
    """
    startTime = time.time()
    result = []
    nb_iter = int(len(sirenList)/100)+1

    for i in range(nb_iter):
    #for i in range(3):

        # reconexion toutes les 300 itérations pour éviter tout soucis de déconnexion
        if (i%300==0):
            token = get_token(userId)
            if token == "Erreur de connexion" :
                logging.info(token)
                return token
            else :
                logging.info('Connexion effectuée')

        logging.info(' ')
        logging.info(f'itereration : {i+1} / {nb_iter}')

        # séparation de la liste de recherche siren en batch de 100 entreprises (nombre max de siren recherchée en une requête)
        if i == int(len(sirenList)/100):
            sirenBatch = sirenList[100*i:]
        else : 
            sirenBatch = sirenList[100*i:100*(i+1)]
        sirenSearchBatch = ''.join(['&siren[]='+sirenNum for sirenNum in sirenBatch])

        endpoint = f"https://registre-national-entreprises.inpi.fr/api/companies?&pageSize=100{sirenSearchBatch}"
        headers = {"Authorization": "Bearer "+ token}

        startTimeRecherche = time.time()

        try:     
            responseList = requests.get(endpoint, headers=headers).json()

            if type(responseList) == dict: 
                # cas des requêtes de trop grandes tailles
                if 'code' in responseList.keys() and responseList['code'] == '500':
                    logging.info('Requête trop grande : subdivise en petite requête')
                    sirenSearchMiniBatchList = [sirenBatch[i:i + 10] for i in range(0, len(sirenBatch), 10)]
                    sub_endpointList = [f"https://registre-national-entreprises.inpi.fr/api/companies?&pageSize=10{sirenSearchMiniBatch}" for sirenSearchMiniBatch in sirenSearchMiniBatchList]
                    responseList = [requests.get(sub_endpoint, headers=headers).json() for sub_endpoint in sub_endpointList]
                    responseList = [item for sublist in responseList for item in sublist]
                else:
                    logging.info('Erreur : change user')
                    return 'Change user'
            resultResponseList, result = process_response(responseList, result)

        except:

            # cas ou il y a un problème de connexion
            time.sleep(10)
            token = get_token(userId)
            r = 1
            while token == "Erreur de connexion" :
                token = get_token(userId+r) # change d'utilisateur 
                r += 1
                if r == 10 :
                    logging.info('Fail')
                    return result[:-1]
            headers = {"Authorization": "Bearer "+ token}
            responseList = requests.get(endpoint, headers=headers).json()
            resultResponseList, result = process_response(responseList, result)
            logging.info('Erreur : résolue')

            # cas où les appels à l'api ne fonctionne plus                 
            try :
                test = (type(responseList) == list) and (type(responseList[0]) == dict)
            except :
                return result[:-1]
            s = 1
            while test == False :
                time.sleep(10)
                responseList = requests.get(endpoint, headers=headers).json()
                test = (type(responseList) == list) and (type(responseList[0]) == dict)
                s+= 1
                logging.info('Erreur : connexion à API')
                if s == 3 :
                    logging.info('Fail')
                    return result[:-1]
            startTimeRecherche = time.time()
            resultResponseList, result = process_response(responseList, result)
            logging.info('Erreur : résolue')

        logging.info(f'Taille recherche entreprises : {len(resultResponseList)}')
        logging.info(f'Duréee recherche entreprises : {round(time.time() - startTimeRecherche, 2)} s  /  Duréee totale : {round(time.time() - startTime, 2)} s')
        
    return result
