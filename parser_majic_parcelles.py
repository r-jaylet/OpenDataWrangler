"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (parcelles) de la base parcelles

    Documentation
    -------
        Description générale parcelles : https://www.data.gouv.fr/fr/datasets/base-parcelles-des-entreprises-et-de-leurs-etablissements-parcelles-siret/
        
"""
import logging
import os
import sys

import pandas as pd

from utils.utils_majic import download_file_majic_parcelles

logger = logging.getLogger('parcelleslogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Parcelles:

    #static
    COLUMN_NAME_ID_PARCELLE = "idParcelle"
    COLUMN_NAME_DEPARTEMENT = "codeDepartementParcelle"
    COLUMN_NAME_DIRECTION = "codeDirectionParcelle"
    COLUMN_NAME_CODE_COMMUNE = "codeCommuneParcelle"
    COLUMN_NAME_COMMUNE = "communeParcelle"
    COLUMN_NAME_PREFIXE = "prefixeParcelle"
    COLUMN_NAME_SECTION = "sectionParcelle"
    COLUMN_NAME_NUMERO_PLAN = "numeroPlanParcelle"
    COLUMN_NAME_NUMERO_VOIRIE = "numeroVoirieParcelle"
    COLUMN_NAME_INDICE_REPETITION = "indiceRepetitionParcelle"
    COLUMN_NAME_MAJIC_CODE_VOIE = "majicCodeVoieParcelle"
    COLUMN_NAME_RIVOLI_CODE_VOIE = "rivoliCodeVoieParcelle"
    COLUMN_NAME_NATURE_VOIE = "natureVoieParcelle"
    COLUMN_NAME_NUMERO_VOIE = "numeroVoieParcelle"
    COLUMN_CONTENANCE = "contenanceParcelle"
    COLUMN_SUF = "suf"
    COLUMN_NATURE_CULTURE_SUF = "natureCultureSuf"
    COLUMN_CONTENANCE_SUF = "contenanceSuf"
    COLUMN_CODE_DROIT_SUF = "codeDroitSuf"
    COLUMN_MAJIC_PROPRIETAIRE_SUF = "majicProprietaireSuf"
    COLUMN_SIREN_PROPRIETAIRE_SUF = "sirenProprietaireSuf"
    COLUMN_FORME_JURIDIQUE_PROPRIETAIRE_SUF = "formeJuridiqueProprietaireSuf"
    COLUMN_FORME_JURIDIQUE_ABREGE_PROPRIETAIRE_SUF = "formeJuridiqueAbregeProprietaireSuf"
    COLUMN_DENOMINATION_PROPRIETAIRE_SUF = "denominationProprietaireSuf"


    def __init__(self,
                 path_root: str = r".",
                 file_name: str = "rnc-data-gouv-with-qpv.csv",
                 year: str = "2022"):
        """constructor
        """
        self.path_root = path_root
        self.file_name = file_name
        self.year = year

        # chargement initial des dictionnaires
        self.parcelles_df = self.load_parcelles_df()

    def load_parcelles_df(self):
        """Charge le context des données parcelles
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process parcelles")
        try:
            # checks if document is present in file. If not, triggers the download function
            try:
                if self.file_name not in os.listdir(self.path_root):
                    logging.info("Download parcelles")
                    download_file_majic_parcelles(self.path_root, self.year)
                else:
                    logging.info("parcelles already downloaded")
            except:
                logging.info("Download parcelles")
                download_file_majic_parcelles(self.path_root, self.year)

            logging.info("Upload parcelles")
            path_file = os.path.join(self.path_root, self.file_name)
            parcelles_df = pd.read_csv(path_file,
                                   dtype={'Siret représentant légal (si existe)' : str},
                                   sep=',', nrows=10000)

            if parcelles_df is not None:
                logging.info("Processing parcelles")
                # Changement des noms des colonnes
                parcelles_df = parcelles_df.rename(
                    columns={'Département (Champ géographique)': self.COLUMN_NAME_DEPARTEMENT,
                            'Code Direction (Champ géographique)': self.COLUMN_NAME_DIRECTION,
                            'Code Commune (Champ géographique)': self.COLUMN_NAME_CODE_COMMUNE,
                            'Nom Commune (Champ géographique)': self.COLUMN_NAME_COMMUNE,
                            'Préfixe (Références cadastrales)': self.COLUMN_NAME_PREFIXE,
                            'Section (Références cadastrales)': self.COLUMN_NAME_SECTION,
                            'N° plan (Références cadastrales)': self.COLUMN_NAME_NUMERO_PLAN,
                            'N° de voirie (Adresse parcelle)': self.COLUMN_NAME_NUMERO_VOIRIE,
                            'Indice de répétition (Adresse parcelle)': self.COLUMN_NAME_INDICE_REPETITION,
                            'Code voie MAJIC (Adresse parcelle)': self.COLUMN_NAME_MAJIC_CODE_VOIE,
                            'Code voie rivoli (Adresse parcelle)': self.COLUMN_NAME_RIVOLI_CODE_VOIE,
                            'Nature voie (Adresse parcelle)': self.COLUMN_NAME_NATURE_VOIE,
                            'Nom voie (Adresse parcelle)': self.COLUMN_NAME_NUMERO_VOIE,
                            'Contenance (Caractéristiques parcelle)': self.COLUMN_CONTENANCE,
                            'SUF (Evaluation SUF)': self.COLUMN_SUF,
                            'Nature culture (Evaluation SUF)': self.COLUMN_NATURE_CULTURE_SUF,
                            'Contenance (Evaluation SUF)': self.COLUMN_CONTENANCE_SUF,
                            'Code droit (Propriétaire(s) parcelle)': self.COLUMN_CODE_DROIT_SUF,
                            'N° MAJIC (Propriétaire(s) parcelle)': self.COLUMN_MAJIC_PROPRIETAIRE_SUF,
                            'N° SIREN (Propriétaire(s) parcelle)': self.COLUMN_SIREN_PROPRIETAIRE_SUF,
                            'Groupe personne (Propriétaire(s) parcelle)': self.COLUMN_FORME_JURIDIQUE_PROPRIETAIRE_SUF,
                            'Forme juridique (Propriétaire(s) parcelle)': self.COLUMN_FORME_JURIDIQUE_ABREGE_PROPRIETAIRE_SUF,
                            'Forme juridique abrégée (Propriétaire(s) parcelle)': self.COLUMN_FORME_JURIDIQUE_ABREGE_PROPRIETAIRE_SUF,
                            'Dénomination (Propriétaire(s) parcelle)': self.COLUMN_DENOMINATION_PROPRIETAIRE_SUF})

                # Nomenclature des natures cultures des suf
                dict_nature_culture_suf = {'AB': 'Terrains à bâtir',
                                        'AG': 'Terrains d’agrément',
                                        'B': 'Bois',
                                        'BF': 'Futaies Feuillues',
                                        'BM': 'Futaies Mixtes',
                                        'BO': 'Oseraies',
                                        'BP': 'Peupleraies',
                                        'BR': 'Futaies résineuses',
                                        'BS': 'Taillis sous Futaies',
                                        'BT': 'Taillis simples',
                                        'CA': 'Carrières',
                                        'CH': 'Chemins de fer, Canaux de Navigation',
                                        'E': 'Eaux',
                                        'J': 'Jardins',
                                        'L': 'Landes',
                                        'LB': 'Landes Boisées',
                                        'P': 'Prés',
                                        'PA': 'Pâtures ou Pâturages',
                                        'PC': 'Pacages ou Pâtis',
                                        'PE': 'Prés d’embouche',
                                        'PH': 'Herbages',
                                        'PP': 'Prés, Pâtures ou Herbages plantes',
                                        'S': 'Sols',
                                        'T': 'Terre',
                                        'TP': 'Terres plantées',
                                        'VE': 'Vergers',
                                        'VI': 'Vignes',
                }

                parcelles_df[self.COLUMN_NATURE_CULTURE_SUF] = parcelles_df[self.COLUMN_NATURE_CULTURE_SUF].apply(
                    lambda x: dict_nature_culture_suf[x] if type(x) == str and x in dict_nature_culture_suf.keys() else x)
                code_suf = pd.DataFrame(data = {self.COLUMN_NATURE_CULTURE_SUF : dict_nature_culture_suf.keys(),'nomNatureCulture' : dict_nature_culture_suf.values()})
                parcelles_df = parcelles_df.merge(code_suf, how='left', on=self.COLUMN_NATURE_CULTURE_SUF)
                
                del parcelles_df[self.COLUMN_NATURE_CULTURE_SUF]
                parcelles_df.rename(columns = {'nomNatureCulture' : self.COLUMN_NATURE_CULTURE_SUF},inplace=True)
                
                # Nomenclature des codes droit des propriétaires des suf
                dict_code_droit_proprietaires_suf = {
                    'P': 'Propriétaire',
                    'U': 'Usufruitier (associé avec N)',
                    'N': 'Nu-propriétaire (associé avec U)',
                    'B': 'Bailleur à construction (associé avec R)',
                    'R': 'Preneur à construction (associé avec B)',
                    'F': 'Foncier (associé avec D ou T)',
                    'T': 'Ténuyer (associé avec F)',
                    'D': 'Domanier (associé avec F)',
                    'V': 'Bailleur d’un bail à réhabilitation (associé avec W)',
                    'W': 'Preneur d’un bail à réhabilitation (associé avec V)',
                    'A': 'Locataire-Attributaire (associé avec P)',
                    'E': 'Emphytéote (associé avec P)',
                    'K': 'Antichrésiste (associé avec P)',
                    'L': 'Fonctionnaire logé',
                    'G': 'Gérant, mandataire, gestionnaire',
                    'S': 'Syndic de copropriété',
                    'H': 'Associé dans une société en transparence fiscale (associé avec P)',
                    'O': 'Autorisation d’occupation temporaire (70 ans)',
                    'J': 'Jeune agriculteur',
                    'Q': 'Gestionnaire taxe sur les bureaux (Île-de-France)',
                    'X': 'La Poste Occupant et propriétaire',
                    'Y': 'La Poste Occupant et non propriétaire',
                    'C': 'Fiduciaire',
                    'M': 'Occupant d’une parcelle appartenant au département de Mayotte ou à l’Etat (associé à P)'
                }
                code_droit_proprietaires = pd.DataFrame(data = {self.COLUMN_CODE_DROIT_SUF : dict_code_droit_proprietaires_suf.keys(),'nomenclatureCodeDroit' : dict_code_droit_proprietaires_suf.values()})
                parcelles_df = parcelles_df.merge(code_droit_proprietaires,how='left',on=self.COLUMN_CODE_DROIT_SUF)
                del parcelles_df[self.COLUMN_CODE_DROIT_SUF]
                parcelles_df.rename(columns = {'nomenclatureCodeDroit' : self.COLUMN_CODE_DROIT_SUF},inplace=True)

                # Création de l'identifiant de la parcelle
                parcelles_df[self.COLUMN_NAME_PREFIXE].fillna('   ',inplace=True)
                parcelles_df[self.COLUMN_NAME_PREFIXE] = parcelles_df[self.COLUMN_NAME_PREFIXE].replace('   ', '000')
                parcelles_df[self.COLUMN_NAME_SECTION] = parcelles_df[self.COLUMN_NAME_SECTION].fillna('NA')
                parcelles_df[self.COLUMN_NAME_SECTION] = parcelles_df[self.COLUMN_NAME_SECTION].apply(lambda x: '0' + x if len(str(x)) == 1 else x)
                parcelles_df[self.COLUMN_NAME_ID_PARCELLE] = parcelles_df[self.COLUMN_NAME_DEPARTEMENT] + parcelles_df[self.COLUMN_NAME_CODE_COMMUNE] + parcelles_df[self.COLUMN_NAME_PREFIXE]  + parcelles_df[self.COLUMN_NAME_SECTION] + parcelles_df[self.COLUMN_NAME_NUMERO_PLAN]
                parcelles_df = parcelles_df[[self.COLUMN_NAME_ID_PARCELLE]+[col for col in parcelles_df.columns if col!=self.COLUMN_NAME_ID_PARCELLE]]

                # correction du type des adresses des locaux
                parcelles_df[self.COLUMN_NAME_NUMERO_VOIRIE] = pd.to_numeric(parcelles_df[self.COLUMN_NAME_NUMERO_VOIRIE], errors='coerce').fillna(0).astype(int)
                parcelles_df[self.COLUMN_NAME_CODE_COMMUNE] = parcelles_df[self.COLUMN_NAME_DEPARTEMENT].astype(str) + parcelles_df[self.COLUMN_NAME_CODE_COMMUNE].astype(str).apply(lambda x:x.zfill(3))
                parcelles_df = parcelles_df[[self.COLUMN_NAME_ID_PARCELLE,
                                    self.COLUMN_NAME_DEPARTEMENT,
                                    self.COLUMN_NAME_CODE_COMMUNE,
                                    self.COLUMN_NAME_COMMUNE,
                                    self.COLUMN_NAME_PREFIXE,
                                    self.COLUMN_NAME_SECTION,
                                    self.COLUMN_NAME_NUMERO_PLAN,
                                    self.COLUMN_NAME_NUMERO_VOIRIE,
                                    self.COLUMN_NAME_INDICE_REPETITION,
                                    self.COLUMN_NAME_MAJIC_CODE_VOIE,
                                    self.COLUMN_NAME_RIVOLI_CODE_VOIE,
                                    self.COLUMN_NAME_NATURE_VOIE,
                                    self.COLUMN_NAME_NUMERO_VOIE,
                                    self.COLUMN_CONTENANCE,
                                    self.COLUMN_SUF,
                                    self.COLUMN_NATURE_CULTURE_SUF,
                                    self.COLUMN_CONTENANCE_SUF,
                                    self.COLUMN_CODE_DROIT_SUF,
                                    self.COLUMN_MAJIC_PROPRIETAIRE_SUF,
                                    self.COLUMN_SIREN_PROPRIETAIRE_SUF,
                                    self.COLUMN_DENOMINATION_PROPRIETAIRE_SUF]]

                return parcelles_df.copy()
            
        except Exception as e:
            logging.error('Erreur chargement base : %s', str(e))

    def get_parcelles_df(self):
        return self.parcelles_df
