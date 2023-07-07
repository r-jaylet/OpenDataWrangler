"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les établissements (siret) de la base SIRENE

    Documentation
    -------
        Description générale SIRENE : https://www.data.gouv.fr/fr/datasets/base-sirete-des-entreprises-et-de-leurs-etablissements-siret-siret/

"""
import logging
import sys

import pandas as pd

logger = logging.getLogger('siretlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Siret:

    def __init__(self, dataframe_siret):
        """constructor
        Parameters
        ----------
        dataframe_siret : dataframe
            dataframe des siret récupérés
        """

        # chargement initial des dictionnaires
        self.dataframe_siret = dataframe_siret.copy()
        self.siret_df = self.load_siret_df()

    def load_siret_df(self):
        """Charge le context des données siret
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process SIRET")
        # on garde les établissements ouverts uniquement et on enlève les établissements situés à l'étranger
        siret_df = self.dataframe_siret[(self.dataframe_siret.etatAdministratifEtablissement == "A") &
                                        (self.dataframe_siret.libelleCommuneEtrangerEtablissement.isna())]

        # identification type activitée
        siret_df['codeTypeActivitePrincipaleEtablissement'] = siret_df['activitePrincipaleEtablissement'].apply(
            lambda x: x.split('.')[0] if type(x) == str else x)
        dict_type_activitees = {
            'AGRICULTURE, SYLVICULTURE ET PÊCHE': ['01', '02', '03'],
            'INDUSTRIES EXTRACTIVES': ['05', '06', '07', '08', '09'],
            'INDUSTRIE MANUFACTURIÈRE':
            ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27',
             '28', '29', '30', '31', '32', '33'],
            "PRODUCTION ET DISTRIBUTION D'ÉLECTRICITÉ, DE GAZ, DE VAPEUR ET D'AIR CONDITIONNÉ": ['35'],
            "PRODUCTION ET DISTRIBUTION D'EAU ; ASSAINISSEMENT, GESTION DES DÉCHETS ET DÉPOLLUTION":
            ['36', '37', '38', '39'],
            "CONSTRUCTION": ['41', '42', '43'],
            "COMMERCE ; RÉPARATION D'AUTOMOBILES ET DE MOTOCYCLES": ['45', '46', '47'],
            "TRANSPORTS ET ENTREPOSAGE": ['49', '50', '51', '52', '53'],
            "HÉBERGEMENT ET RESTAURATION": ['55', '56'],
            "INFORMATION ET COMMUNICATION": ['58', '59', '60', '61', '62', '63'],
            "ACTIVITÉS FINANCIÈRES ET D'ASSURANCE": ['64', '65', '66'],
            "ACTIVITÉS IMMOBILIÈRES": ['68'],
            "ACTIVITÉS SPÉCIALISÉES, SCIENTIFIQUES ET TECHNIQUES": ['69', '70', '71', '72', '73', '74', '75'],
            "ACTIVITÉS DE SERVICES ADMINISTRATIFS ET DE SOUTIEN": ['77', '78', '79', '80', '81', '82'],
            "ADMINISTRATION PUBLIQUE": ['84'],
            "ENSEIGNEMENT": ['85'],
            "SANTÉ HUMAINE ET ACTION SOCIALE": ['86', '87', '88'],
            "ARTS, SPECTACLES ET ACTIVITÉS RÉCRÉATIVES": ['90', '91', '92', '93'],
            "AUTRES ACTIVITÉS DE SERVICES": ['94', '95', '96'],
            "ACTIVITÉS DES MÉNAGES EN TANT QU'EMPLOYEURS ; ACTIVITÉS INDIFFÉRENCIÉES DES MÉNAGES EN TANT QUE PRODUCTEURS DE BIENS ET SERVICES POUR USAGE PROPRE":
            ['97', '98'],
            "ACTIVITÉS EXTRA-TERRITORIALES": ['99']}
        data_type_activitees = []
        for activity, codes in dict_type_activitees.items():
            for code in codes:
                data_type_activitees.append([activity, code])
        df_type_activitees = pd.DataFrame(
            data_type_activitees,
            columns=['typeActivitePrincipaleEtablissement', 'codeTypeActivitePrincipaleEtablissement'])
        siret_df = siret_df.merge(
            df_type_activitees, how='left', on='codeTypeActivitePrincipaleEtablissement').drop(
            'codeTypeActivitePrincipaleEtablissement', axis=1)

        
        # identification type effectifs
        effectifCode = {'NN': 'NN',
                        '00': '0',
                        '01': '1-2',
                        '02': '3-5',
                        '03': '6-9',
                        '11': '10-19',
                        '12': '20-49',
                        '21': '50-99',
                        '22': '100-199',
                        '31': '200-249',
                        '32': '250-499',
                        '41': '500-999',
                        '42': '1000-1999',
                        '51': '2000-4999',
                        '52': '5000-9999',
                        '53': '10000+'}
        df_type_effectif = pd.DataFrame.from_dict(effectifCode, orient='index', columns=['significationTranche'])
        siret_df = pd.merge(siret_df, df_type_effectif, left_on='trancheEffectifsEtablissement', right_index=True, how='left')
        siret_df = siret_df.drop('trancheEffectifsEtablissement', axis=1)
        siret_df = siret_df.rename({'significationTranche': 'trancheEffectifsEtablissement'}, axis=1)

        # format dates
        siret_df['dateCreationEtablissement'] = pd.to_datetime(siret_df['dateCreationEtablissement'], errors='coerce')
        siret_df['dateCreationEtablissement'] = siret_df['dateCreationEtablissement'].replace('1900-01-01', pd.NA)
        siret_df['dateDernierTraitementEtablissement'] = pd.to_datetime(siret_df['dateDernierTraitementEtablissement'], errors='coerce')
        siret_df['dateDernierTraitementEtablissement'] = siret_df['dateDernierTraitementEtablissement'].dt.strftime('%Y-%m-%d')

        # format names
        siret_df['denominationUsuelleEtablissement'] = siret_df['denominationUsuelleEtablissement'].str.upper()

        # ajout departement
        siret_df['codeDepartementEtablissement'] = siret_df['codeCommuneEtablissement'].apply(lambda x: str(x)[:2])

        # post processing
        siret_df = siret_df[['siren',
                             'siret',
                             'denominationUsuelleEtablissement',
                             'dateCreationEtablissement',
                             'dateDernierTraitementEtablissement',
                             'trancheEffectifsEtablissement',
                             'etablissementSiege',
                             'complementAdresseEtablissement',
                             'numeroVoieEtablissement',
                             'indiceRepetitionEtablissement',
                             'typeVoieEtablissement',
                             'libelleVoieEtablissement',
                             'codePostalEtablissement',
                             'codeCommuneEtablissement',
                             'codeDepartementEtablissement',
                             'activitePrincipaleEtablissement',
                             'nomenclatureActivitePrincipaleEtablissement',
                             'typeActivitePrincipaleEtablissement']]

        return siret_df.copy()

    def get_siret_df(self):
        return self.siret_df
