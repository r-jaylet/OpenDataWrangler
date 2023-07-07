"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les personnes morales propriétaires des parcelles (base majic)

    Documentation
    -------
        Description générale MAJIC : https://www.data.gouv.fr/fr/datasets/fichiers-des-locaux-et-des-parcelles-des-personnes-morales/
 
"""
import sys
import logging

import pandas as pd

logger = logging.getLogger('majicparcelleslogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Majic_parcelles:
    
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

    def __init__(self, dataframe_parcelles):
        """constructeur
        Parameters
        -------------------
        dataframe_parcelles : dataframe
            dataframe des données des locaux concatenés
        """

        # chargement initial des dictionnaires
        self.dataframe_parcelles = dataframe_parcelles.copy()
        self.parcelles_df = self.process_data_majic_parcelles()

    def process_data_majic_parcelles(self):
        """Charge le context des données MAJIC PARCELLES
        Parameters
        ----------------
        df_parc non traité
        Returns
        ----------------
        df_parc traité
        """

        logging.info("Process MAJIC PARCELLES")
        df_parc = self.dataframe_parcelles
        # Changement des noms des colonnes
        df_parc = df_parc.rename(
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

        df_parc[self.COLUMN_NATURE_CULTURE_SUF] = df_parc[self.COLUMN_NATURE_CULTURE_SUF].apply(
            lambda x: dict_nature_culture_suf[x] if type(x) == str and x in dict_nature_culture_suf.keys() else x)
        code_suf = pd.DataFrame(data = {self.COLUMN_NATURE_CULTURE_SUF : dict_nature_culture_suf.keys(),'nomNatureCulture' : dict_nature_culture_suf.values()})
        df_parc = df_parc.merge(code_suf, how='left', on=self.COLUMN_NATURE_CULTURE_SUF)
        
        del df_parc[self.COLUMN_NATURE_CULTURE_SUF]
        df_parc.rename(columns = {'nomNatureCulture' : self.COLUMN_NATURE_CULTURE_SUF},inplace=True)
        
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
        df_parc = df_parc.merge(code_droit_proprietaires,how='left',on=self.COLUMN_CODE_DROIT_SUF)
        del df_parc[self.COLUMN_CODE_DROIT_SUF]
        df_parc.rename(columns = {'nomenclatureCodeDroit' : self.COLUMN_CODE_DROIT_SUF},inplace=True)

        # Création de l'identifiant de la parcelle
        df_parc[self.COLUMN_NAME_PREFIXE].fillna('   ',inplace=True)
        df_parc[self.COLUMN_NAME_PREFIXE] = df_parc[self.COLUMN_NAME_PREFIXE].replace('   ', '000')
        df_parc[self.COLUMN_NAME_SECTION] = df_parc[self.COLUMN_NAME_SECTION].fillna('NA')
        df_parc[self.COLUMN_NAME_SECTION] = df_parc[self.COLUMN_NAME_SECTION].apply(lambda x: '0' + x if len(str(x)) == 1 else x)
        df_parc[self.COLUMN_NAME_ID_PARCELLE] = df_parc[self.COLUMN_NAME_DEPARTEMENT] + df_parc[self.COLUMN_NAME_CODE_COMMUNE] + df_parc[self.COLUMN_NAME_PREFIXE]  + df_parc[self.COLUMN_NAME_SECTION] + df_parc[self.COLUMN_NAME_NUMERO_PLAN]
        df_parc = df_parc[[self.COLUMN_NAME_ID_PARCELLE]+[col for col in df_parc.columns if col!=self.COLUMN_NAME_ID_PARCELLE]]

        # correction du type des adresses des locaux
        df_parc[self.COLUMN_NAME_NUMERO_VOIRIE] = pd.to_numeric(df_parc[self.COLUMN_NAME_NUMERO_VOIRIE], errors='coerce').fillna(0).astype(int)
        df_parc[self.COLUMN_NAME_CODE_COMMUNE] = df_parc[self.COLUMN_NAME_DEPARTEMENT].astype(str) + df_parc[self.COLUMN_NAME_CODE_COMMUNE].astype(str).apply(lambda x:x.zfill(3))
        df_parc = df_parc[[self.COLUMN_NAME_ID_PARCELLE,
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





        return df_parc.copy()

    def get_majic_parcelles_df(self):
        return self.parcelles_df
