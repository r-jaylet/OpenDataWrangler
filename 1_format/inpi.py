import requests
import pandas as pd
import datetime
import time
import json
import requests


# fetch token for specfic account on INPI
def get_token():
    endpoint = "https://registre-national-entreprises.inpi.fr/api/sso/login"
    body = json.dumps({"username": "remi.jaylet@hotmail.com", "password": "Denver85Fran!" })
    response = requests.post(endpoint, data=body).json()
    return response['token']


def search_dirigeant(token, siren):
    # call to api
    endpoint = "https://registre-national-entreprises.inpi.fr/api/companies/" + str(siren)
    headers = {"Authorization": "Bearer " + token}
    response = requests.get(endpoint, headers=headers).json()

    # dataframe for each possible responsible entity
    df_entreprises = pd.DataFrame()  # liste entreprises actionnaires
    df_individus = pd.DataFrame()  # liste individus actionnaires
    df_beneficiaires = pd.DataFrame()  # liste beneficaires

    if 'formality' in response:
        personne = response['formality']['content']
        if 'personneMorale' in personne.keys():
            personne = personne['personneMorale']
        elif 'personnePhysique' in personne.keys():
            personne = personne['personnePhysique']
        # get beneficiaire
        beneficiairesEffectifs = personne['beneficiairesEffectifs']
        beneficiaires = [benef['beneficiaire']['descriptionPersonne'] for benef in beneficiairesEffectifs]
        df_beneficiaires = pd.DataFrame(beneficiaires)

        # get composition (ind. and entreprise)
        individu_personne = []
        individu_domicile = []
        entreprise = []

        if 'composition' in personne:
            composition = personne['composition']
            for i in range(len(composition['pouvoirs'])):
                if list(composition['pouvoirs'][i].keys())[0] == 'individu':
                    if 'descriptionPersonne' in composition['pouvoirs'][i]['individu']:
                        individu_personne.append(
                            composition['pouvoirs'][i]['individu']['descriptionPersonne'])  # info on person
                    if 'adresseDomicile' in composition['pouvoirs'][i]['individu']:
                        individu_domicile.append(
                            composition['pouvoirs'][i]['individu']['adresseDomicile'])  # info on location
                elif list(composition['pouvoirs'][i].keys())[0] == 'entreprise':
                    if 'entreprise' in composition['pouvoirs'][i]:
                        entreprise.append(composition['pouvoirs'][i]['entreprise'])  # info on company
            df_entreprises = pd.DataFrame(entreprise)
            df_individu_personne = pd.DataFrame(individu_personne)
            df_individu_domicile = pd.DataFrame(individu_domicile)
            df_individus = df_individu_personne.merge(df_individu_domicile, how='left', left_index=True,
                                                      right_index=True)

    return df_beneficiaires, df_entreprises, df_individus
