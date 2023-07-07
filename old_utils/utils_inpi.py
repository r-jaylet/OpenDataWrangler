"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les représentants d'entreprises de l'API de l'INPI

    Documentation
    -------
        Description générale INPI : https://data.inpi.fr/
                
"""
import logging
import sys

import numpy as np
import pandas as pd

logger = logging.getLogger('inpilogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Inpi:

    #static
    COLUMN_NAME_ID_ANNONCE = "updatedAt"
    COLUMN_NAME_ID_ANNONCE = "siren"
    COLUMN_NAME_TYPE_POUVOIR = "typePouvoir"
    COLUMN_NAME_INDIVIDU_DATENAISSANCE = "individuDateDeNaissance"
    COLUMN_NAME_INDIVIDU_NOM = "individuNom"
    COLUMN_NAME_INDIVIDU_NOM_USAGE = "individuNomUsage"
    COLUMN_NAME_INDIVIDU_PRENOMS = "individuPrenoms"
    COLUMN_NAME_INDIVIDU_PRENOM1 = "individuPrenom1"
    COLUMN_NAME_INDIVIDU_DATE_EFFET = "individuDateEffetRoleDeclarant"
    COLUMN_NAME_INDIVIDU_NATIONALITE = "individuNationalite"
    COLUMN_NAME_INDIVIDU_SITUATION_MATRIMONIALE = "individuSituationMatrimoniale"
    COLUMN_NAME_INDIVIDU_ROLE = "individuRole"
    COLUMN_NAME_ENTREPRISE_SIREN = "entrepriseSiren"
    COLUMN_NAME_ENTREPRISE_ROLE = "entrepriseRole"
    COLUMN_NAME_ENTREPRISE_DENOMINATION = "entrepriseDenomination"

    def __init__(self, dataframe_inpi):
        """constructor
        Parameters
        ----------
        dataframe_inpi : dataframe
            dataframe concatené de tous les données reccueillies de l'inpi par département
        """

        # chargement initial des dictionnaires
        self.dataframe_inpi = dataframe_inpi.copy()
        self.inpi_df = self.load_inpi_df()

     

    def load_inpi_df(self):
        """Charge le context des données inpi
        Parameters
        -------
        Returns
        -------
            df
        """
        logging.info("Process INPI")
        

        # enleve les roles non reliés à des fonctions de dirigeants
        self.dataframe_inpi = self.dataframe_inpi.loc[((self.dataframe_inpi['typePouvoir'] == 'individu') & ~(self.dataframe_inpi['individuNom'].isna())) | (
            (self.dataframe_inpi['typePouvoir'] == 'entreprise') & ~(self.dataframe_inpi['entrepriseSiren'].isna()))]
        list_uninterested = [11, 13, 14, 71, 72, 76, 77, 86, 90, 97, 98, 99, 94, 101, 103, 104, 105, 106]
        self.dataframe_inpi = self.dataframe_inpi[~self.dataframe_inpi['individuRole'].isin(list_uninterested)]
        self.dataframe_inpi = self.dataframe_inpi[~self.dataframe_inpi['entrepriseRole'].isin(list_uninterested)]

        # correspondace classe role dirigiant
        roleCode = {'11': 'Membre',
                    '13': 'Contrôleur de gestion',
                    '14': 'Contrôleur des comptes',
                    '23': 'Autre associé majoritaire',
                    '28': 'Gérant et associé indéfiniment et solidairement responsable',
                    '29': 'Gérant et associé indéfiniment responsable',
                    '30': 'Gérant',
                    '40': 'Liquidateur',
                    '41': 'Associé unique (qui participe à l’activité EURL)',
                    '51': 'Président du conseil d’administration',
                    '52': 'Président du directoire',
                    '53': 'Directeur Général',
                    '55': 'Dirigeant à l’étranger d’une personne morale étrangère',
                    '56': 'Dirigeant en France d’une personne morale étrangère',
                    '60': 'Président du conseil d’administration et directeur général',
                    '61': 'Président du conseil de surveillance',
                    '63': 'Membre du directoire',
                    '64': 'Membre du conseil de surveillance',
                    '65': 'Administrateur',
                    '66': 'Personne ayant le pouvoir d’engager à titre habituel la société',
                    '67': "Personne ayant le pouvoir d’engager l'établissement",
                    '69': 'Directeur général unique de SA à directoire',
                    '70': 'Directeur général délégué',
                    '71': 'Commissaire aux comptes titulaire',
                    '72': 'Commissaire aux comptes suppléant',
                    '73': 'Président de SAS',
                    '74': 'Associé indéfiniment et solidairement responsable',
                    '75': 'Associé indéfiniment responsable',
                    '76': 'Représentant social en France d’une entreprise étrangère',
                    '77': 'Représentant fiscal en France d’une entreprise étrangère',
                    '82': 'Indivisaire',
                    '86': 'Exploitant pour le compte de l’indivision',
                    '90': 'Personne physique, exploitant en commun',
                    '97': 'Mandataire ad hoc',
                    '98': 'Administrateur provisoire',
                    '99': 'Autre',
                    '94': 'Membre non salarié participant aux travaux',
                    '95': 'Associé qui participe à la gestion',
                    '96': 'Associé non salarié',
                    '100': 'Repreneur',
                    '101': 'Entrepreneur',
                    '103': 'Suppléant',
                    '104': 'Personne chargée du contrôle',
                    '105': 'Personne décisionnaire désignée',
                    '106': 'Comptable',
                    '107': 'Héritier indivisaire',
                    '108': 'Loueur'}
        # ajout des codes créés
        roleCode['0'] = 'Représentant par défaut'
        roleCode['1'] = 'Représentant avec rôle non précisé'

        code_label_mapping = {int(code): label for code, label in roleCode.items()}
        self.dataframe_inpi['entrepriseRole'] = self.dataframe_inpi['entrepriseRole'].map(code_label_mapping)
        self.dataframe_inpi['individuRole'] = self.dataframe_inpi['individuRole'].map(code_label_mapping)

        # formating des dates
        self.dataframe_inpi['updatedAt'] = pd.to_datetime(self.dataframe_inpi['updatedAt'])
        self.dataframe_inpi['individuDateDeNaissance'] = pd.to_datetime(self.dataframe_inpi['individuDateDeNaissance'], errors='coerce')
        self.dataframe_inpi['individuDateEffetRoleDeclarant'] = pd.to_datetime(
            self.dataframe_inpi['individuDateEffetRoleDeclarant'], errors='coerce')
        self.dataframe_inpi.loc[(self.dataframe_inpi['individuDateDeNaissance'].dt.year <= 1900) |
                    (self.dataframe_inpi['individuDateDeNaissance'].dt.year >= 2023),
                    'individuDateDeNaissance'] = np.nan

        # formatting nom dirigeant
        self.dataframe_inpi['individuNom'] = self.dataframe_inpi['individuNom'].str.upper()
        self.dataframe_inpi['individuPrenom1'] = self.dataframe_inpi['individuPrenom1'].str.upper()
        self.dataframe_inpi['individuNationalite'] = self.dataframe_inpi['individuNationalite'].str.upper()
        self.dataframe_inpi = self.dataframe_inpi.drop_duplicates()

        return self.dataframe_inpi.copy()

    def get_inpi_df(self):
        return self.inpi_df
