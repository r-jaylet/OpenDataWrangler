"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les personnes morales propriétaires des locaux (base majic)

    Documentation
    -------
        Description générale MAJIC : https://www.data.gouv.fr/fr/datasets/fichiers-des-locaux-et-des-parcelles-des-personnes-morales/

"""
import logging
import sys

import pandas as pd

logger = logging.getLogger('majiclocauxlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Majic_locaux:
    
    #static
    COLUMN_NAME_ID_PARCELLE = "idParcelle"
    COLUMN_NAME_DEPARTEMENT = "codeDepartementParcelle"
    COLUMN_NAME_DIRECTION = "codeDirectionParcelle"
    COLUMN_NAME_CODE_COMMUNE = "codeCommuneParcelle"
    COLUMN_NAME_COMMUNE = "communeParcelle"
    COLUMN_NAME_PREFIXE = "prefixeParcelle"
    COLUMN_NAME_SECTION = "sectionParcelle"
    COLUMN_NAME_NUMERO_PLAN = "numeroPlanParcelle"
    COLUMN_NAME_BATIMENT = "batimentLocal"
    COLUMN_NAME_ENTREE = "entreeLocal"
    COLUMN_NAME_NIVEAU = "niveauLocal"
    COLUMN_NAME_PORTE = "porteLocal"
    COLUMN_NAME_NUMERO_VOIRIE = "numeroVoirieLocal"
    COLUMN_NAME_INDICE_REPETITION = "indiceRepetitionLocal"
    COLUMN_NAME_MAJIC_CODE_VOIE = "majicCodeVoieLocal"
    COLUMN_NAME_RIVOLI_CODE_VOIE = "rivoliCodeVoieLocal"
    COLUMN_NAME_NATURE_VOIE = "natureVoieLocal"
    COLUMN_NAME_NOM_VOIE = "nomVoieLocal"
    COLUMN_CODE_DROIT = "codeDroitLocal"
    COLUMN_MAJIC_PROPRIETAIRE = "majicProprietaireLocal"
    COLUMN_SIREN_PROPRIETAIRE = "sirenProprietaireLocal"
    COLUMN_DENOMINATION_PROPRIETAIRE = "denominationProprietaireLocal"

    def __init__(self, dataframe_locaux):
        """constructeur
        Parameters
        -------------------
        dataframe_locaux : dataframe
            dataframe des données des locaux concatenés
        """
        
        # chargement initial des dictionnaires
        self.dataframe_locaux = dataframe_locaux.copy()
        self.locaux_df = self.process_data_majic_locaux()

    def process_data_majic_locaux(self):
        """Charge le context des données MAJIC LOCAUX
        Parameters
        ----------------
        df_loc non traitée
        Returns
        ----------------
        df_loc traitée
        """

        logging.info("Process MAJIC LOCAUX")
        df_loc = self.dataframe_locaux
        # Changement des noms des colonnes
        df_loc = df_loc.rename(
            columns={'Département (Champ géographique)': self.COLUMN_NAME_DEPARTEMENT,
                     'Code Direction (Champ géographique)': self.COLUMN_NAME_DIRECTION,
                     'Code Commune (Champ géographique)': self.COLUMN_NAME_CODE_COMMUNE,
                     'Nom Commune (Champ géographique)': self.COLUMN_NAME_COMMUNE,
                     'Préfixe (Références cadastrales)': self.COLUMN_NAME_PREFIXE,
                     'Section (Références cadastrales)': self.COLUMN_NAME_SECTION,
                     'N° plan (Références cadastrales)': self.COLUMN_NAME_NUMERO_PLAN,
                     'Bâtiment (Identification du local)': self.COLUMN_NAME_BATIMENT,
                     'Entrée (Identification du local)': self.COLUMN_NAME_ENTREE,
                     'Niveau (Identification du local)': self.COLUMN_NAME_NIVEAU,
                     'Porte (Identification du local)': self.COLUMN_NAME_PORTE,
                     'N° voirie (Adresse du local)': self.COLUMN_NAME_NUMERO_VOIRIE,
                     'Indice de répétition (Adresse du local)': self.COLUMN_NAME_INDICE_REPETITION,
                     'Code voie MAJIC (Adresse du local)': self.COLUMN_NAME_MAJIC_CODE_VOIE,
                     'Code voie rivoli (Adresse du local)': self.COLUMN_NAME_RIVOLI_CODE_VOIE,
                     'Nature voie (Adresse du local)': self.COLUMN_NAME_NATURE_VOIE,
                     'Nom voie (Adresse du local)': self.COLUMN_NAME_NOM_VOIE,
                     'Code droit (Propriétaire(s) du local)': self.COLUMN_CODE_DROIT,
                     'N° MAJIC (Propriétaire(s) du local)': self.COLUMN_MAJIC_PROPRIETAIRE,
                     'N° SIREN (Propriétaire(s) du local)': self.COLUMN_SIREN_PROPRIETAIRE,
                     'Dénomination (Propriétaire(s) du local)': self.COLUMN_DENOMINATION_PROPRIETAIRE})
        
        # Nomenclature des codes droit des propriétaires des suf
        dict_code_droit_proprietaires = {
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
        code_droit_proprietaires = pd.DataFrame(data = {self.COLUMN_CODE_DROIT : dict_code_droit_proprietaires.keys(),'nomenclatureCodeDroit' : dict_code_droit_proprietaires.values()})
        df_loc = df_loc.merge(code_droit_proprietaires, how='left', on=self.COLUMN_CODE_DROIT)
        del df_loc[self.COLUMN_CODE_DROIT]
        df_loc.rename(columns = {'nomenclatureCodeDroit' : self.COLUMN_CODE_DROIT}, inplace=True)
        
        # Création de l'identifiant de la parcelle
        df_loc[self.COLUMN_NAME_PREFIXE].fillna('   ', inplace=True)

        # enlève les parcelles de l'ourtre mer
        df_loc[~df_loc[self.COLUMN_NAME_DEPARTEMENT].isin(['97', '98'])]

        # création de l'identifiant de la parcelle
        df_loc[self.COLUMN_NAME_SECTION] = df_loc[self.COLUMN_NAME_SECTION].fillna('NA')
        df_loc[self.COLUMN_NAME_PREFIXE] = df_loc[self.COLUMN_NAME_PREFIXE].replace('   ', '000')
        df_loc[self.COLUMN_NAME_SECTION] = df_loc[self.COLUMN_NAME_SECTION].apply(lambda x: '0' + x if len(str(x)) == 1 else x)
        df_loc[self.COLUMN_NAME_ID_PARCELLE] = df_loc[self.COLUMN_NAME_DEPARTEMENT] + df_loc[self.COLUMN_NAME_CODE_COMMUNE] + \
            df_loc[self.COLUMN_NAME_PREFIXE] + df_loc[self.COLUMN_NAME_SECTION] + df_loc[self.COLUMN_NAME_NUMERO_PLAN]
        df_loc = df_loc[[self.COLUMN_NAME_ID_PARCELLE]+[col for col in df_loc.columns if col != self.COLUMN_NAME_ID_PARCELLE]]

        # correction du type des adresses des locaux
        df_loc[self.COLUMN_NAME_NUMERO_VOIRIE] = pd.to_numeric(df_loc[self.COLUMN_NAME_NUMERO_VOIRIE], errors='coerce').fillna(0).astype(int)
        df_loc[self.COLUMN_NAME_CODE_COMMUNE] = df_loc[self.COLUMN_NAME_DEPARTEMENT].astype(str) + df_loc[self.COLUMN_NAME_CODE_COMMUNE].astype(str).apply(lambda x: x.zfill(3))
        df_loc[self.COLUMN_NAME_ENTREE] = pd.to_numeric(df_loc[self.COLUMN_NAME_ENTREE], errors='coerce').fillna(0).astype(int)
        df_loc[self.COLUMN_NAME_NIVEAU] = pd.to_numeric(df_loc[self.COLUMN_NAME_NIVEAU], errors='coerce').fillna(0).astype(int)
        df_loc[self.COLUMN_NAME_PORTE] = pd.to_numeric(df_loc[self.COLUMN_NAME_PORTE], errors='coerce').fillna(0).astype(int)
 
        df_loc = df_loc[[self.COLUMN_NAME_ID_PARCELLE,
                            self.COLUMN_NAME_DEPARTEMENT,
                            self.COLUMN_NAME_CODE_COMMUNE,
                            self.COLUMN_NAME_COMMUNE,
                            self.COLUMN_NAME_PREFIXE,
                            self.COLUMN_NAME_SECTION,
                            self.COLUMN_NAME_NUMERO_PLAN,
                            self.COLUMN_NAME_BATIMENT,
                            self.COLUMN_NAME_ENTREE,
                            self.COLUMN_NAME_NIVEAU,
                            self.COLUMN_NAME_PORTE,
                            self.COLUMN_NAME_NUMERO_VOIRIE,
                            self.COLUMN_NAME_INDICE_REPETITION,
                            self.COLUMN_NAME_MAJIC_CODE_VOIE,
                            self.COLUMN_NAME_RIVOLI_CODE_VOIE,
                            self.COLUMN_NAME_NATURE_VOIE,
                            self.COLUMN_NAME_NOM_VOIE,
                            self.COLUMN_CODE_DROIT,
                            self.COLUMN_MAJIC_PROPRIETAIRE,
                            self.COLUMN_SIREN_PROPRIETAIRE,
                            self.COLUMN_DENOMINATION_PROPRIETAIRE]]   
      
        return df_loc.copy()

    def get_majic_locaux_df(self):
        return self.locaux_df
