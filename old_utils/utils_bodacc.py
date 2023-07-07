"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données issues des annonces commerciales de la version BETA de l'API de BODACC

    Documentation
    -------
        Description générale BODACC : https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/information/?sort=dateparution

"""
import datetime
import locale
import logging
import sys

import pandas as pd

logger = logging.getLogger('bodaccloggging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Bodacc:

    # static
    COLUMN_NAME_ID_ANNONCE = "idAnnonce"
    COLUMN_NAME_DEPARTEMENT = "departement"
    COLUMN_NAME_DATE_PARUTION_ANNONCE = "dateParutionAnnonce"
    COLUMN_NAME_NUMERO_ANNONCE = "numeroAnnonce"
    COLUMN_NAME_TYPE_AVIS = "typeavis"
    COLUMN_NAME_FAMILLE_AVIS = "familleavis"
    COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE = "numeroDepartementTribunalAnnonce"
    COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE = "codeRegionTribunalAnnonce"
    COLUMN_NAME_TRIBUNAL_ANNONCE = "tribunalAnnonce"
    COLUMN_NAME_SIREN_ANNONCE = "sirenAnnonce"

    COLUMN_NAME_DATE_JUGEMENT_ANNONCE = "dateJugementAnnonceProceduresCollectives"
    COLUMN_NAME_TYPE_JUGEMENT_ANNONCE = "typeJugementAnnonceProceduresCollectives"
    COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE = "familleJugementAnnonceProceduresCollectives"
    COLUMN_NAME_NATURE_JUGEMENT_ANNONCE = "natureJugementAnnonceProceduresCollectives"
    COLUMN_NAME_COMPLEMENT_JUGEMENT_ANNONCE = "complementJugementAnnonceProceduresCollectives"

    COLUMN_NAME_NOM_PUBLICATION_INITIALE = "nomPublicationInitiale"
    COLUMN_NAME_DATE_PUBLICATION_INITIALE = "datePublicationInitiale"
    COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE = "numeroParutionPublicationInitiale"
    COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE = "numeroAnnoncePublicationInitiale"

    COLUMN_NAME_DESCRIPTIF_ANNONCE = 'descriptifAnnonceVentesCessions'
    COLUMN_NAME_DEATE_COMMENCENT_ACTIVITE_ANNONCE = 'dateCommencementActiviteAnnonceVentesCessions'
    COLUMN_NAME_DATE_IMMATRICULATION_ANNONCE = 'dateImmatriculationAnnonceVentesCessions'
    COLUMN_NAME_VENTE_ANNONCE = 'venteAnnonceVentesCessions'

    COLUMN_NAME_RADIATION_PP_ANNONCE = 'radiationPPAnnonceRadiation'
    COLUMN_NAME_RADIATION_PM_ANNONCE = 'radiationPMAnnonceRadiation'
    COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE = 'radiationCommentaireAnnonceRadiation'
    COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PP_ANNONCE = 'dateCessationActivitePPAnnonceRadiation'
    COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PM_ANNONCE = 'dateCessationActivitePMAnnonceRadiation'

    def __init__(self,
                 bodacc_PC_initial_df, bodacc_PC_rectificatif_df, bodacc_PC_annulation_df,
                 bodacc_VC_initial_df, bodacc_VC_rectificatif_df, bodacc_VC_annulation_df,
                 bodacc_RADIATION_initial_df, bodacc_RADIATION_rectificatif_df, bodacc_RADIATION_annulation_df):
        """constructor
        Parameters
        ----------
            les dataframes des différents scopes BODACC
        """

        # chargement initial des dictionnaires
        self.bodacc_PC_initial_df = bodacc_PC_initial_df.copy()
        self.bodacc_PC_rectificatif_df = bodacc_PC_rectificatif_df.copy()
        self.bodacc_PC_annulation_df = bodacc_PC_annulation_df.copy()
        self.bodacc_VC_initial_df = bodacc_VC_initial_df.copy()
        self.bodacc_VC_rectificatif_df = bodacc_VC_rectificatif_df.copy()
        self.bodacc_VC_annulation_df = bodacc_VC_annulation_df.copy()
        self.bodacc_RADIATION_initial_df = bodacc_RADIATION_initial_df.copy()
        self.bodacc_RADIATION_rectificatif_df = bodacc_RADIATION_rectificatif_df.copy()
        self.bodacc_RADIATION_annulation_df = bodacc_RADIATION_annulation_df.copy()

        # Traitement des fichiers des procédures collectives
        self.bodacc_PC_initial_df = self.process_bodacc_PC_df(self.bodacc_PC_initial_df, type_avis='initial')
        self.bodacc_PC_rectificatif_df = self.process_bodacc_PC_df(
            self.bodacc_PC_rectificatif_df, type_avis='rectificatif')
        self.bodacc_PC_annulation_df = self.process_bodacc_PC_df(self.bodacc_PC_annulation_df, type_avis='annulation')

        # Traitement des fichiers des ventes et des cessions
        self.bodacc_VC_initial_df = self.process_bodacc_VC_df(self.bodacc_VC_initial_df, type_avis='initial')
        self.bodacc_VC_rectificatif_df = self.process_bodacc_VC_df(
            self.bodacc_VC_rectificatif_df, type_avis='rectificatif')
        self.bodacc_VC_annulation_df = self.process_bodacc_VC_df(self.bodacc_VC_annulation_df, type_avis='annulation')

        # Traitement des fichiers des radiations
        self.bodacc_RADIATION_initial_df = self.process_bodacc_radiation_df(
            self.bodacc_RADIATION_initial_df, type_avis='initial')
        self.bodacc_RADIATION_rectificatif_df = self.process_bodacc_radiation_df(
            self.bodacc_RADIATION_rectificatif_df, type_avis='rectificatif')
        self.bodacc_RADIATION_annulation_df = self.process_bodacc_radiation_df(
            self.bodacc_RADIATION_annulation_df, type_avis='annulation')

        self.bodacc_df = self.concat_bodacc_df()

     # dictionnaires
    bodacc_df = None

    def process_bodacc_PC_df(self, bodacc_df, type_avis):
        """Traite les données des fichiers bodacc concernant les procédures collectives en incluant notamment les dernières rectifications et annulations
        Parameters
        -------
        Returns
        -------
            df
        """
        logging.info("Process PC")

        if not bodacc_df.empty:

            if type_avis in ['initial', 'rectificatif']:
                # retire les annonces inexploitables
                bodacc_df = bodacc_df.dropna(subset=['jugement', 'registre'])
                # eliminer les siren incorrects
                bodacc_df = bodacc_df.dropna(subset='registre')
                bodacc_df['registre'] = bodacc_df['registre'].astype(str)
                bodacc_df['registre'] = bodacc_df['registre'].apply(lambda x: x.replace('000 000 000,000000000,', ''))
                bodacc_df = bodacc_df[bodacc_df['registre'] != '000 000 000,000000000']

                # recupère info jugement
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df['jugement'].apply(
                    lambda x: x['nature'] if type(x) == dict and 'nature' in x.keys() else '')
                bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE] = bodacc_df['jugement'].apply(
                    lambda x: x['famille'] if type(x) == dict and 'famille' in x.keys() else '')
                bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = bodacc_df['jugement'].apply(
                    lambda x: x['date'] if type(x) == dict and 'date' in x.keys() else '')
                bodacc_df[self.COLUMN_NAME_COMPLEMENT_JUGEMENT_ANNONCE] = bodacc_df['jugement'].apply(
                    lambda x: x['complementJugement'] if type(x) == dict and 'complementJugement' in x.keys() else '')
                bodacc_df = bodacc_df.drop('jugement', axis=1)

                # modification du nom des colonnes
                columns_to_rename = {
                    "id": self.COLUMN_NAME_ID_ANNONCE,
                    "dateparution": self.COLUMN_NAME_DATE_PARUTION_ANNONCE,
                    "numeroannonce": self.COLUMN_NAME_NUMERO_ANNONCE,
                    "region_code": self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE,
                    "numerodepartement": self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                    "tribunal": self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                    "registre": self.COLUMN_NAME_SIREN_ANNONCE
                }
                bodacc_df.rename(columns=columns_to_rename, inplace=True)

                # fix error import
                bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã©', 'é') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã´', 'ô') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã§', 'ç') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ãª', 'ê') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã©', 'é') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã´', 'ô') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã§', 'ç') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ãª', 'ê') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã¨', 'è') if type(x) == str else x)
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('Ã', 'à') if type(x) == str else x)

                # standardisation des formats de date
                locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')  # change interpretation mois langue
                bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE].apply(
                    lambda x: x.replace('1er', '1') if type(x) == str else x)

                def convert_to_uniform_format(date_str):
                    if not date_str:
                        return None
                    try:
                        date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
                        return date.strftime('%Y-%m-%d')
                    except:
                        try:
                            date = datetime.datetime.strptime(date_str, '%d %B %Y')
                            return date.strftime('%Y-%m-%d')
                        except:
                            return None
                        
                bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE].apply(
                    lambda x: convert_to_uniform_format(x))

                # gestion des dates de jugement incorrectes
                date_threshold = datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d')
                bodacc_df.loc[(bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] < '2008-01-01'),self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = None
                bodacc_df.loc[(bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] > date_threshold),self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = None
                bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE].fillna(
                    bodacc_df[self.COLUMN_NAME_DATE_PARUTION_ANNONCE])
                bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = pd.to_datetime(bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE])
                #bodacc_df['year'] = bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE].dt.year
                #bodacc_df.loc[(bodacc_df.year < 2008), self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = None
                #bodacc_df.loc[(bodacc_df.year >= datetime.date.today().year),
                 #             self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = None
                #bodacc_df = bodacc_df.drop('year', axis=1)
                #bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE].fillna(
                  #  bodacc_df[self.COLUMN_NAME_DATE_PARUTION_ANNONCE])
                #bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE] = pd.to_datetime(
                 #   bodacc_df[self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE])

            else:
                # eliminer les siren incorrects
                bodacc_df = bodacc_df.dropna(subset='registre')
                bodacc_df['registre'] = bodacc_df['registre'].astype(str)
                bodacc_df['registre'] = bodacc_df['registre'].apply(lambda x: x.replace('000 000 000,000000000,', ''))
                bodacc_df = bodacc_df[bodacc_df['registre'] != '000 000 000,000000000']

                # recupère info jugement
                bodacc_df[self.COLUMN_NAME_TYPE_JUGEMENT_ANNONCE] = bodacc_df['jugement'].apply(
                    lambda x: x['type'] if type(x) == dict and 'type' in x.keys() else '')
                bodacc_df[self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE] = bodacc_df['jugement'].apply(
                    lambda x: x['famille'] if type(x) == dict and 'famille' in x.keys() else '')
                bodacc_df[self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE] = bodacc_df['jugement'].apply(
                    lambda x: x['nature'] if type(x) == dict and 'nature' in x.keys() else '')
                bodacc_df = bodacc_df.drop('jugement', axis=1)

                # modification du nom des colonnes
                columns_to_rename = {
                    "id": self.COLUMN_NAME_ID_ANNONCE,
                    "numeroannonce": self.COLUMN_NAME_NUMERO_ANNONCE,
                    "dateparution": self.COLUMN_NAME_DATE_PARUTION_ANNONCE,
                    "region_code": self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE,
                    "numerodepartement": self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                    "tribunal": self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                    "registre": self.COLUMN_NAME_SIREN_ANNONCE,
                }
                bodacc_df.rename(columns=columns_to_rename, inplace=True)

            if type_avis in ['rectificatif', 'annulation']:
                bodacc_df[self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE] = bodacc_df['parutionavisprecedent'].apply(
                    lambda x: x['numeroParution'] if type(x) == dict and 'numeroParution' in x.keys() else None)
                bodacc_df[self.COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE] = bodacc_df['parutionavisprecedent'].apply(
                    lambda x: x['numeroAnnonce'] if type(x) == dict and 'numeroAnnonce' in x.keys() else None)
                bodacc_df['idAnnonceARectifierOuAAnnuler'] = 'A' + bodacc_df[self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE].astype(
                    str) + bodacc_df[self.COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE].astype(str)
                bodacc_df = bodacc_df.drop(
                    ['parutionavisprecedent', self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE, self.
                     COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE],
                    axis=1)

            # extraction des num siren
            bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] = bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE].apply(
                lambda x: x.split(','))
            # on perd ici la pertinence des colonnes commerçant et ville
            bodacc_df = bodacc_df.explode(self.COLUMN_NAME_SIREN_ANNONCE)
            bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] = bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE].apply(
                lambda x: x.replace(' ', ''))
            bodacc_df = bodacc_df.drop_duplicates(subset=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE])
            bodacc_df = bodacc_df[bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] != '000000000']
            if type_avis == 'initial':
                bodacc_df = bodacc_df[[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.COLUMN_NAME_NUMERO_ANNONCE,
                                       self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE,
                                       self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE, self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE, self.COLUMN_NAME_COMPLEMENT_JUGEMENT_ANNONCE]]
            elif type_avis == 'rectificatif':
                bodacc_df = bodacc_df[[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                                       COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.
                                       COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE, self.
                                       COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE, self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE,
                                       self.COLUMN_NAME_COMPLEMENT_JUGEMENT_ANNONCE, 'idAnnonceARectifierOuAAnnuler']]
            else:
                bodacc_df = bodacc_df[[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.COLUMN_NAME_NUMERO_ANNONCE,
                                       self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_TYPE_JUGEMENT_ANNONCE,
                                       self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE, self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE, 'idAnnonceARectifierOuAAnnuler']]

        else:
            if type_avis == 'initial':
                bodacc_df = pd.DataFrame(columns=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.COLUMN_NAME_NUMERO_ANNONCE,
                                                  self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                                  self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE,
                                                  self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE, self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE, self.COLUMN_NAME_COMPLEMENT_JUGEMENT_ANNONCE])
            elif type_avis == 'rectificatif':
                bodacc_df = pd.DataFrame(columns=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.COLUMN_NAME_NUMERO_ANNONCE,
                                                  self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                                  self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE,
                                                  self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE, self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE, self.COLUMN_NAME_COMPLEMENT_JUGEMENT_ANNONCE, 'idAnnonceARectifierOuAAnnuler'])
            else:
                bodacc_df = pd.DataFrame(columns=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.COLUMN_NAME_NUMERO_ANNONCE,
                                                  self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                                  self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_TYPE_JUGEMENT_ANNONCE,
                                                  self.COLUMN_NAME_FAMILLE_JUGEMENT_ANNONCE, self.COLUMN_NAME_NATURE_JUGEMENT_ANNONCE, 'idAnnonceARectifierOuAAnnuler'])

        return bodacc_df.copy()

    def process_bodacc_VC_df(self, bodacc_df, type_avis):
        """Traite les données des fichiers bodacc concernant les ventes et les cessions en incluant notamment les dernières rectifications et annulations
        Parameters
        -------
        Returns
        -------
            df
        """
        logging.info("Process VC")

        if not bodacc_df.empty:
            # retire les annonces inexploitables
            if type_avis in ['initial', 'rectificatif']:
                bodacc_df = bodacc_df.dropna(subset=['acte', 'registre'])
            # eliminer les siren incorrects
            bodacc_df = bodacc_df.dropna(subset='registre')
            bodacc_df['registre'] = bodacc_df['registre'].astype(str)
            bodacc_df['registre'] = bodacc_df['registre'].apply(lambda x: x.replace('000 000 000,000000000,', ''))
            bodacc_df = bodacc_df[bodacc_df['registre'] != '000 000 000,000000000']

            # get acte info
            bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE] = bodacc_df['acte'].apply(
                lambda x: x['descriptif'] if type(x) == dict and 'descriptif' in x.keys() else None)
            bodacc_df[self.COLUMN_NAME_DEATE_COMMENCENT_ACTIVITE_ANNONCE] = bodacc_df['acte'].apply(
                lambda x: x['dateCommencementActivite'] if type(x) == dict and 'dateCommencementActivite' in x.keys() else None)
            bodacc_df[self.COLUMN_NAME_DATE_IMMATRICULATION_ANNONCE] = bodacc_df['acte'].apply(
                lambda x: x['dateImmatriculation'] if type(x) == dict and 'dateImmatriculation' in x.keys() else None)
            bodacc_df[self.COLUMN_NAME_VENTE_ANNONCE] = bodacc_df['acte'].apply(
                lambda x: x['vente'] if type(x) == dict and 'vente' in x.keys() else None)
            bodacc_df = bodacc_df.drop('acte', axis=1)

            # modification du nom des colonnes
            columns_to_rename = {
                "id": self.COLUMN_NAME_ID_ANNONCE,
                "dateparution": self.COLUMN_NAME_DATE_PARUTION_ANNONCE,
                "numeroannonce": self.COLUMN_NAME_NUMERO_ANNONCE,
                "region_code": self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE,
                "numerodepartement": self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                "tribunal": self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                "registre": self.COLUMN_NAME_SIREN_ANNONCE
            }
            bodacc_df.rename(columns=columns_to_rename, inplace=True)

            # fix error import
            bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE] = bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE].apply(
                lambda x: x.replace('Ã©', 'é') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE] = bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE].apply(
                lambda x: x.replace('Ã´', 'ô') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE] = bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE].apply(
                lambda x: x.replace('Ã§', 'ç') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE] = bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE].apply(
                lambda x: x.replace('Ãª', 'ê') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE] = bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE].apply(
                lambda x: x.replace('Ã¨', 'è') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE] = bodacc_df[self.COLUMN_NAME_DESCRIPTIF_ANNONCE].apply(
                lambda x: x.replace('Ã', 'à') if type(x) == str else x)

            if type_avis in ['rectificatif', 'annulation']:

                bodacc_df[self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE] = bodacc_df['parutionavisprecedent'].apply(
                    lambda x: x['numeroParution'] if type(x) == dict and 'numeroParution' in x.keys() else None)
                bodacc_df[self.COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE] = bodacc_df['parutionavisprecedent'].apply(
                    lambda x: x['numeroAnnonce'] if type(x) == dict and 'numeroAnnonce' in x.keys() else None)
                bodacc_df['idAnnonceARectifierOuAAnnuler'] = 'A' + bodacc_df[self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE].astype(
                    str) + bodacc_df[self.COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE].astype(str)
                bodacc_df = bodacc_df.drop(
                    ['parutionavisprecedent', self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE, self.
                     COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE],
                    axis=1)

            # Explode après tous les traitements pour qu'ils soient appliqués à moins de lignes

            # extraction des num siren
            bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] = bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE].apply(
                lambda x: x.split(','))
            # on perd ici la pertinence des colonnes commerçant et ville
            bodacc_df = bodacc_df.explode(self.COLUMN_NAME_SIREN_ANNONCE)
            bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] = bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE].apply(
                lambda x: x.replace(' ', ''))
            # le drop duplicates ne marche pas avec des colonnes en dict
            bodacc_df = bodacc_df.drop_duplicates(subset=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE])
            bodacc_df = bodacc_df[bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] != '000000000']
            if type_avis == 'initial':
                bodacc_df = bodacc_df[[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                                       COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.
                                       COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DESCRIPTIF_ANNONCE, self.
                                       COLUMN_NAME_DEATE_COMMENCENT_ACTIVITE_ANNONCE, self.
                                       COLUMN_NAME_DATE_IMMATRICULATION_ANNONCE, self.COLUMN_NAME_VENTE_ANNONCE]]
            else:
                bodacc_df = bodacc_df[[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                                       COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.
                                       COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DESCRIPTIF_ANNONCE, self.
                                       COLUMN_NAME_DEATE_COMMENCENT_ACTIVITE_ANNONCE, self.
                                       COLUMN_NAME_DATE_IMMATRICULATION_ANNONCE, self.COLUMN_NAME_VENTE_ANNONCE,
                                       'idAnnonceARectifierOuAAnnuler']]
        else:
            if type_avis == 'initial':
                bodacc_df = pd.DataFrame(
                    columns=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                             COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS,
                             self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DESCRIPTIF_ANNONCE, self.
                             COLUMN_NAME_DEATE_COMMENCENT_ACTIVITE_ANNONCE, self.
                             COLUMN_NAME_DATE_IMMATRICULATION_ANNONCE, self.COLUMN_NAME_VENTE_ANNONCE])
            else:
                bodacc_df = pd.DataFrame(
                    columns=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                             COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS,
                             self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DESCRIPTIF_ANNONCE, self.
                             COLUMN_NAME_DEATE_COMMENCENT_ACTIVITE_ANNONCE, self.
                             COLUMN_NAME_DATE_IMMATRICULATION_ANNONCE, self.COLUMN_NAME_VENTE_ANNONCE,
                             'idAnnonceARectifierOuAAnnuler'])

        return bodacc_df.copy()

    def process_bodacc_radiation_df(self, bodacc_df, type_avis):
        """Traite les données des fichiers bodacc concernant les radiations en incluant notamment les dernières rectifications et annulations
        Parameters
        -------
        Returns
        -------
            df
        """
        logging.info("Process radiation")

        if not bodacc_df.empty:
            # eliminer les siren incorrects
            bodacc_df = bodacc_df.dropna(subset='registre')
            bodacc_df['registre'] = bodacc_df['registre'].astype(str)
            bodacc_df['registre'] = bodacc_df['registre'].apply(lambda x: x.replace('000 000 000,000000000,', ''))
            bodacc_df = bodacc_df[bodacc_df['registre'] != '000 000 000,000000000']

            # get acte info
            bodacc_df[self.COLUMN_NAME_RADIATION_PP_ANNONCE] = bodacc_df['radiationaurcs'].apply(
                lambda x: x['radiationPP'] if type(x) == dict and 'radiationPP' in x.keys() else None)
            bodacc_df[self.COLUMN_NAME_RADIATION_PM_ANNONCE] = bodacc_df['radiationaurcs'].apply(
                lambda x: x['radiationPM'] if type(x) == dict and 'radiationPM' in x.keys() else None)
            bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE] = bodacc_df['radiationaurcs'].apply(
                lambda x: x['commentaire'] if type(x) == dict and 'commentaire' in x.keys() else None)
            bodacc_df[self.COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PP_ANNONCE] = bodacc_df['radiationaurcs'].apply(
                lambda x: x['dateCessationActivitePP'] if type(x) == dict and 'dateCessationActivitePP' in x.keys() else None)
            bodacc_df[self.COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PM_ANNONCE] = bodacc_df['radiationaurcs'].apply(
                lambda x: x['dateCessationActivitePM'] if type(x) == dict and 'dateCessationActivitePM' in x.keys() else None)
            bodacc_df = bodacc_df.drop('radiationaurcs', axis=1)

            # modification du nom des colonnes
            columns_to_rename = {
                "id": self.COLUMN_NAME_ID_ANNONCE,
                "dateparution": self.COLUMN_NAME_DATE_PARUTION_ANNONCE,
                "numeroannonce": self.COLUMN_NAME_NUMERO_ANNONCE,
                "region_code": self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE,
                "numerodepartement": self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                "tribunal": self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                "registre": self.COLUMN_NAME_SIREN_ANNONCE
            }
            bodacc_df.rename(columns=columns_to_rename, inplace=True)

            # fix error import
            bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE] = bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE].apply(
                lambda x: x.replace('Ã©', 'é') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE] = bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE].apply(
                lambda x: x.replace('Ã´', 'ô') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE] = bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE].apply(
                lambda x: x.replace('Ã§', 'ç') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE] = bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE].apply(
                lambda x: x.replace('Ãª', 'ê') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE] = bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE].apply(
                lambda x: x.replace('Ã¨', 'è') if type(x) == str else x)
            bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE] = bodacc_df[self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE].apply(
                lambda x: x.replace('Ã', 'à') if type(x) == str else x)

            if type_avis in ['rectificatif', 'annulation']:

                bodacc_df[self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE] = bodacc_df['parutionavisprecedent'].apply(
                    lambda x: x['numeroParution'] if type(x) == dict and 'numeroParution' in x.keys() else None)
                bodacc_df[self.COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE] = bodacc_df['parutionavisprecedent'].apply(
                    lambda x: x['numeroAnnonce'] if type(x) == dict and 'numeroAnnonce' in x.keys() else None)
                bodacc_df['idAnnonceARectifierOuAAnnuler'] = 'B' + bodacc_df[self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE].astype(
                    str) + bodacc_df[self.COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE].astype(str)
                bodacc_df = bodacc_df.drop(
                    ['parutionavisprecedent', self.COLUMN_NAME_NUMERO_PARUTION_PUBLICATION_INITIALE, self.
                     COLUMN_NAME_NUMERO_ANNONCE_PUBLICATION_INITIALE],
                    axis=1)

            # Explode après tous les traitements pour qu'ils soient appliqués à moins de lignes
            bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] = bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE].apply(
                lambda x: x.split(','))
            # on perd ici la pertinence des colonnes commerçant et ville
            bodacc_df = bodacc_df.explode(self.COLUMN_NAME_SIREN_ANNONCE)
            bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] = bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE].apply(
                lambda x: x.replace(' ', ''))
            bodacc_df = bodacc_df.drop_duplicates(subset=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_SIREN_ANNONCE])
            bodacc_df = bodacc_df[bodacc_df[self.COLUMN_NAME_SIREN_ANNONCE] != '000000000']
            if type_avis == 'initial':
                bodacc_df = bodacc_df[[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                                       COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.
                                       COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_RADIATION_PP_ANNONCE, self.
                                       COLUMN_NAME_RADIATION_PM_ANNONCE, self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE,
                                       self.COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PP_ANNONCE, self.
                                       COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PM_ANNONCE]]
            else:
                bodacc_df = bodacc_df[[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                                       COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.
                                       COLUMN_NAME_FAMILLE_AVIS, self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE,
                                       self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_RADIATION_PP_ANNONCE, self.
                                       COLUMN_NAME_RADIATION_PM_ANNONCE, self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE,
                                       self.COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PP_ANNONCE, self.
                                       COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PM_ANNONCE,
                                       'idAnnonceARectifierOuAAnnuler']]
        else:
            if type_avis == 'initial':
                bodacc_df = pd.DataFrame(
                    columns=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                             COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS,
                             self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_RADIATION_PP_ANNONCE, self.
                             COLUMN_NAME_RADIATION_PM_ANNONCE, self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE, self.
                             COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PP_ANNONCE, self.
                             COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PM_ANNONCE])
            else:
                bodacc_df = pd.DataFrame(
                    columns=[self.COLUMN_NAME_ID_ANNONCE, self.COLUMN_NAME_DATE_PARUTION_ANNONCE, self.
                             COLUMN_NAME_NUMERO_ANNONCE, self.COLUMN_NAME_TYPE_AVIS, self.COLUMN_NAME_FAMILLE_AVIS,
                             self.COLUMN_NAME_NUMERO_DEPARTEMENT_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_CODE_REGION_TRIBUNAL_ANNONCE, self.COLUMN_NAME_TRIBUNAL_ANNONCE, self.
                             COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_RADIATION_PP_ANNONCE, self.
                             COLUMN_NAME_RADIATION_PM_ANNONCE, self.COLUMN_NAME_RADIATION_COMMENTAIRE_ANNONCE, self.
                             COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PP_ANNONCE, self.
                             COLUMN_NAME_RADIATION_DATE_CESSATION_ACTIVITE_PM_ANNONCE, 'idAnnonceARectifierOuAAnnuler'])

        return bodacc_df.copy()

    def concat_bodacc_df(self):
        logging.info("Merge")
        ouverture_sauvegarde = ["Jugement d'ouverture d'une procédure de sauvegarde",
                                "Jugement d'ouverture d'une procédure de sauvegarde financière accélérée",
                                "Jugement d'extension d'une procédure de sauvegarde",
                                "Jugement d'ouverture d'une procédure de sauvegarde accélérée"]
        ouverture_redressement = [
            "Jugement d'ouverture d'une procédure de redressement judiciaire", 'Jugement de plan de redressement',
            'Jugement de conversion en redressement judiciaire de la procédure de sauvegarde',
            'Jugement prononçant la résolution du plan de sauvegarde et le redressement judiciaire',
            'Jugement prononçant la résolution du plan de sauvegarde financière accélérée et le redressement judiciaire',
            'Jugement de conversion en redressement judiciaire de la procédure de sauvegarde financière accélérée',
            'Jugement mettant fin à la procédure de traitement de sortie de crise et ouvrant une procédure de redressement judiciaire',
            'Jugement prononçant la résolution du plan de traitement de sortie de crise et le redressement judiciaire']
        nature_redressement_fin = ['Jugement mettant fin à la procédure de redressement judiciaire',
                                   'Jugement prononçant la résolution du plan de redressement']
        ouverture_liquidation = [
            "Jugement d'ouverture de liquidation judiciaire", 'Jugement de conversion en liquidation judiciaire',
            'Jugement de conversion en liquidation judiciaire de la procédure de sauvegarde',
            'Jugement prononçant la résolution du plan de cession et la liquidation judiciaire',
            'Jugement prononçant la résolution du plan de sauvegarde et la liquidation judiciaire',
            'Jugement prononçant la résolution du plan de sauvegarde financière accélérée et la liquidation judiciaire',
            'Jugement prononçant la résolution du plan de redressement et la liquidation judiciaire',
            'Jugement prononçant la résolution du plan de sauvegarde accélérée et la liquidation judiciaire',
            'Jugement mettant fin à la procédure de traitement de sortie de crise et ouvrant une procédure de liquidation judiciaire',
            'Jugement prononçant la résolution du plan de traitement de sortie de crise et la liquidation judiciaire',
            'Jugement prononçant la résolution du plan de redressement et la liquidation judiciaire',
            'Jugement de reprise de la procédure de liquidation judiciaire',
            'Jugement prononçant la liquidation judiciaire']
        arret_sauvegarde = [
            'Jugement arrêtant le plan de sauvegarde', 'Jugement mettant fin à la procédure de sauvegarde',
            'Jugement arrêtant le plan de sauvegarde financière accélérée',
            'Jugement arrêtant le plan de sauvegarde accélérée',
            'Jugement prononçant la résolution du plan de sauvegarde et la liquidation judiciaire',
            'Jugement de conversion en redressement judiciaire de la procédure de sauvegarde',
            'Jugement de conversion en liquidation judiciaire de la procédure de sauvegarde']
        arret_redressement = [
            'Jugement mettant fin à la procédure de redressement judiciaire',
            'Jugement prononçant la résolution du plan de redressement et la liquidation judiciaire',
            'Jugement prononçant la résolution du plan de redressement',
            'Jugement prononçant la résolution du plan de cession et la liquidation judiciaire',
            'Jugement prononçant la résolution du plan de sauvegarde et le redressement judiciaire']
        self.bodacc_PC_initial_df['ouvertureSauvegarde'] = 0
        self.bodacc_PC_initial_df['ouvertureRedressement'] = 0
        self.bodacc_PC_initial_df['ouvertureLiquidation'] = 0
        self.bodacc_PC_initial_df.loc[self.bodacc_PC_initial_df['natureJugementAnnonceProceduresCollectives'].isin(
            ouverture_sauvegarde),
            'ouvertureSauvegarde'] = 1
        self.bodacc_PC_initial_df.loc[self.bodacc_PC_initial_df['natureJugementAnnonceProceduresCollectives'].isin(
            ouverture_redressement),
            'ouvertureRedressement'] = 1
        self.bodacc_PC_initial_df.loc[self.bodacc_PC_initial_df['natureJugementAnnonceProceduresCollectives'].isin(
            ouverture_liquidation),
            'ouvertureLiquidation'] = 1
        df_pc_cloture = self.bodacc_PC_initial_df[(self.bodacc_PC_initial_df
                                                   ['natureJugementAnnonceProceduresCollectives'].isin(
                                                       arret_sauvegarde + arret_redressement) |
                                                   (self.bodacc_PC_initial_df
                                                    ['familleJugementAnnonceProceduresCollectives'] ==
                                                    "Jugement de clôture"))]
        df_pc_cloture.drop_duplicates(subset='sirenAnnonce', inplace=True)
        df_pc_cloture.rename(columns={'dateParutionAnnonce': 'dateParutionAnnonceCloture'}, inplace=True)
        self.bodacc_PC_initial_df = self.bodacc_PC_initial_df.merge(
            df_pc_cloture[['sirenAnnonce', 'dateParutionAnnonceCloture']],
            how='left', on='sirenAnnonce')
        self.bodacc_PC_initial_df['procedureCollectiveClotureePotentiellement'] = self.bodacc_PC_initial_df[
            'dateParutionAnnonce'] <= self.bodacc_PC_initial_df['dateParutionAnnonceCloture']
        self.bodacc_PC_initial_df['procedureCollectiveClotureePotentiellement'].fillna(0, inplace=True)
        bodacc_pc_df = pd.concat(
            [self.bodacc_PC_initial_df, self.bodacc_PC_rectificatif_df, self.bodacc_PC_annulation_df])
        bodacc_vc_df = pd.concat(
            [self.bodacc_VC_initial_df, self.bodacc_VC_rectificatif_df, self.bodacc_VC_annulation_df])
        bodacc_vc_df = bodacc_vc_df[(bodacc_vc_df['sirenAnnonce'].isin(bodacc_pc_df['sirenAnnonce'].unique()))]
        bodacc_radiation_df = pd.concat([self.bodacc_RADIATION_initial_df,
                                        self.bodacc_RADIATION_rectificatif_df, self.bodacc_RADIATION_annulation_df])
        bodacc_radiation_df = bodacc_radiation_df[(bodacc_radiation_df['sirenAnnonce'].isin(
            bodacc_pc_df['sirenAnnonce'].unique()))]
        bodacc_df = pd.concat([bodacc_pc_df, bodacc_vc_df, bodacc_radiation_df])
        bodacc_df = bodacc_df.sort_values(
            by=[self.COLUMN_NAME_SIREN_ANNONCE, self.COLUMN_NAME_DATE_JUGEMENT_ANNONCE,
                self.COLUMN_NAME_DATE_PARUTION_ANNONCE],
            ignore_index=True, ascending=True)
        bodacc_df = bodacc_df[['sirenAnnonce']+[col for col in bodacc_df.columns if col != 'sirenAnnonce']]

        return bodacc_df

    def get_bodacc_df(self):
        return self.bodacc_df
