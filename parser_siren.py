"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (siren) de la base SIRENE

    Documentation
    -------
        Description générale SIRENE : https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
        
"""
import logging
import os
import sys

import pandas as pd

from utils.utils_sirene import download_file_sirene

logger = logging.getLogger('sirenlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Siren:

    def __init__(self,
                 path_root: str = r".",
                 file_name: str = "StockUniteLegale_utf8.csv"):
        """constructor
        """
        self.path_root = path_root
        self.file_name = file_name

        # chargement initial des dictionnaires
        self.siren_df = self.load_siren_df()

    def load_siren_df(self):
        """Charge le context des données siren
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process SIREN")

        try:

            # checks if document is present in file. If not, triggers the download function
            try:
                if self.file_name not in os.listdir(self.path_root):
                    logging.info("Download Siren")
                    download_file_sirene(self.path_root, 'UniteLegale')
                else:
                    logging.info("Siren already downloaded")
            except:
                logging.info("Download Siren")
                download_file_sirene(self.path_root, 'UniteLegale')

            logging.info("Upload Siren")
            path_file = os.path.join(self.path_root, self.file_name)
            siren_df = pd.read_csv(path_file,
                                   dtype={'siren': str,
                                          'trancheEffectifsUniteLegale': str,
                                          'categorieJuridiqueUniteLegale': str,
                                          'nicSiegeUniteLegale': str,
                                          'activiteUniteLegale': str},
                                   sep=',', nrows=10000)

            if siren_df is not None:
                logging.info("Processing Siren")
                # élimination des unités purgées et cessées
                siren_df = siren_df[(siren_df.unitePurgeeUniteLegale.isna()) & (siren_df.etatAdministratifUniteLegale == 'A')]

                # identification type activitée
                siren_df['codeTypeActivitePrincipaleUniteLegale'] = siren_df['activitePrincipaleUniteLegale'].apply(lambda x: x.split('.')[
                                                                                                                    0] if type(x) == str else x)
                dict_type_activitees = {
                    'AGRICULTURE, SYLVICULTURE ET PÊCHE': ['01', '02', '03'],
                    'INDUSTRIES EXTRACTIVES': ['05', '06', '07', '08', '09'],
                    'INDUSTRIE MANUFACTURIÈRE': ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33'],
                    "PRODUCTION ET DISTRIBUTION D'ÉLECTRICITÉ, DE GAZ, DE VAPEUR ET D'AIR CONDITIONNÉ": ['35'],
                    "PRODUCTION ET DISTRIBUTION D'EAU ; ASSAINISSEMENT, GESTION DES DÉCHETS ET DÉPOLLUTION": ['36', '37', '38', '39'],
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
                    "ACTIVITÉS DES MÉNAGES EN TANT QU'EMPLOYEURS ; ACTIVITÉS INDIFFÉRENCIÉES DES MÉNAGES EN TANT QUE PRODUCTEURS DE BIENS ET SERVICES POUR USAGE PROPRE": ['97', '98'],
                    "ACTIVITÉS EXTRA-TERRITORIALES": ['99']}
                data_type_activitees = []
                for activity, codes in dict_type_activitees.items():
                    for code in codes:
                        data_type_activitees.append([activity, code])
                df_type_activitees = pd.DataFrame(
                    data_type_activitees, columns=['typeActivitePrincipaleUniteLegale', 'codeTypeActivitePrincipaleUniteLegale'])

                siren_df = siren_df.merge(
                    df_type_activitees, how='left', on='codeTypeActivitePrincipaleUniteLegale').drop(
                    'codeTypeActivitePrincipaleUniteLegale', axis=1)

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
                siren_df = pd.merge(siren_df, df_type_effectif, left_on='trancheEffectifsUniteLegale',
                                    right_index=True, how='left')
                siren_df = siren_df.drop('trancheEffectifsUniteLegale', axis=1)
                siren_df = siren_df.rename({'significationTranche': 'trancheEffectifsUniteLegale'}, axis=1)

                # format data
                siren_df['nicSiegeUniteLegale'] = siren_df['nicSiegeUniteLegale'].astype(str).apply(lambda x: x.zfill(5))

                # format dates
                siren_df['dateCreationUniteLegale'] = pd.to_datetime(siren_df['dateCreationUniteLegale'], errors='coerce')
                siren_df['dateCreationUniteLegale'] = siren_df['dateCreationUniteLegale'].replace('1900-01-01', pd.NA)
                siren_df['dateDernierTraitementUniteLegale'] = pd.to_datetime(siren_df['dateDernierTraitementUniteLegale'], errors='coerce')
                siren_df['dateDernierTraitementUniteLegale'] = siren_df['dateDernierTraitementUniteLegale'].dt.strftime('%Y-%m-%d')

                # format names
                siren_df['prenom1UniteLegale'] = siren_df['prenom1UniteLegale'].str.upper()
                siren_df['prenom2UniteLegale'] = siren_df['prenom2UniteLegale'].str.upper()
                siren_df['prenom3UniteLegale'] = siren_df['prenom3UniteLegale'].str.upper()
                siren_df['prenom4UniteLegale'] = siren_df['prenom4UniteLegale'].str.upper()
                siren_df['nomUniteLegale'] = siren_df['nomUniteLegale'].str.upper()
                siren_df['nomUsageUniteLegale'] = siren_df['nomUsageUniteLegale'].str.upper()
                siren_df['denominationUniteLegale'] = siren_df['denominationUniteLegale'].str.upper()
                siren_df['prenomUsuelUniteLegale'] = siren_df['prenomUsuelUniteLegale'].str.upper()
                siren_df['pseudonymeUniteLegale'] = siren_df['pseudonymeUniteLegale'].str.upper()

                return siren_df.copy()

        except Exception as e:
            logging.error('Erreur chargement base : %s', str(e))

    def get_siren_df(self):
        return self.siren_df
