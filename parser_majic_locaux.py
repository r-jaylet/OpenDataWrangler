"""City & You Open Data Use Case Exploration

    Summary
    -------
        Manipulation des données sur les unités légales (locaux) de la base locaux

    Documentation
    -------
        Description générale locaux : https://www.data.gouv.fr/fr/datasets/base-locaux-des-entreprises-et-de-leurs-etablissements-locaux-siret/
        
"""
import logging
import os
import sys

import pandas as pd

from utils.utils_majic import download_file_majic_locaux

logger = logging.getLogger('locauxlogging')

# Définition du logging level
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logging.basicConfig(level=logging.INFO)


class Locaux:

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
        self.locaux_df = self.load_locaux_df()

    def load_locaux_df(self):
        """Charge le context des données locaux
        Parameters
        -------
        Returns
        -------
            df
        """

        logging.info("Process locaux")
        try:
            # checks if document is present in file. If not, triggers the download function
            try:
                if self.file_name not in os.listdir(self.path_root):
                    logging.info("Download locaux")
                    download_file_majic_locaux(self.path_root, self.year)
                else:
                    logging.info("locaux already downloaded")
            except:
                logging.info("Download locaux")
                download_file_majic_locaux(self.path_root, self.year)

            logging.info("Upload locaux")
            path_file = os.path.join(self.path_root, self.file_name)
            locaux_df = pd.read_csv(path_file,
                                   dtype={'Siret représentant légal (si existe)' : str},
                                   sep=',', nrows=10000)

            if locaux_df is not None:
                logging.info("Processing locaux")
                
                # Changement des noms des colonnes
                locaux_df = locaux_df.rename(
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
                locaux_df = locaux_df.merge(code_droit_proprietaires, how='left', on=self.COLUMN_CODE_DROIT)
                del locaux_df[self.COLUMN_CODE_DROIT]
                locaux_df.rename(columns = {'nomenclatureCodeDroit' : self.COLUMN_CODE_DROIT}, inplace=True)
                
                # Création de l'identifiant de la parcelle
                locaux_df[self.COLUMN_NAME_PREFIXE].fillna('   ', inplace=True)

                # création de l'identifiant de la parcelle
                locaux_df[self.COLUMN_NAME_SECTION] = locaux_df[self.COLUMN_NAME_SECTION].fillna('NA')
                locaux_df[self.COLUMN_NAME_PREFIXE] = locaux_df[self.COLUMN_NAME_PREFIXE].replace('   ', '000')
                locaux_df[self.COLUMN_NAME_SECTION] = locaux_df[self.COLUMN_NAME_SECTION].apply(lambda x: '0' + x if len(str(x)) == 1 else x)
                locaux_df[self.COLUMN_NAME_ID_PARCELLE] = locaux_df[self.COLUMN_NAME_DEPARTEMENT] + locaux_df[self.COLUMN_NAME_CODE_COMMUNE] + \
                    locaux_df[self.COLUMN_NAME_PREFIXE] + locaux_df[self.COLUMN_NAME_SECTION] + locaux_df[self.COLUMN_NAME_NUMERO_PLAN]
                locaux_df = locaux_df[[self.COLUMN_NAME_ID_PARCELLE]+[col for col in locaux_df.columns if col != self.COLUMN_NAME_ID_PARCELLE]]

                # correction du type des adresses des locaux
                locaux_df[self.COLUMN_NAME_NUMERO_VOIRIE] = pd.to_numeric(locaux_df[self.COLUMN_NAME_NUMERO_VOIRIE], errors='coerce').fillna(0).astype(int)
                locaux_df[self.COLUMN_NAME_CODE_COMMUNE] = locaux_df[self.COLUMN_NAME_DEPARTEMENT].astype(str) + locaux_df[self.COLUMN_NAME_CODE_COMMUNE].astype(str).apply(lambda x: x.zfill(3))
                locaux_df[self.COLUMN_NAME_ENTREE] = pd.to_numeric(locaux_df[self.COLUMN_NAME_ENTREE], errors='coerce').fillna(0).astype(int)
                locaux_df[self.COLUMN_NAME_NIVEAU] = pd.to_numeric(locaux_df[self.COLUMN_NAME_NIVEAU], errors='coerce').fillna(0).astype(int)
                locaux_df[self.COLUMN_NAME_PORTE] = pd.to_numeric(locaux_df[self.COLUMN_NAME_PORTE], errors='coerce').fillna(0).astype(int)
        
                locaux_df = locaux_df[[self.COLUMN_NAME_ID_PARCELLE,
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

                return locaux_df.copy()
            
        except Exception as e:
            logging.error('Erreur chargement base : %s', str(e))

    def get_locaux_df(self):
        return self.locaux_df
