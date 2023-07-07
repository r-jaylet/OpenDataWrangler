import pandas as pd
import glob


def parcelles_format():
    # extract data from txt files
    df_parc1 = pd.concat([pd.read_csv(i, sep=';', encoding='latin', low_memory=False) for i in glob.glob(
        "../0_data/parcelles/Fichier des parcelles (situation 2022)-dept 01 à 61/Fichier des parcelles (situation 2022)-dpts 01 à 61/*.txt",
        recursive=True)])
    df_parc2 = pd.concat([pd.read_csv(i, sep=';', encoding='latin', low_memory=False) for i in glob.glob(
        "../0_data/parcelles/Fichier des parcelles (situation 2022)-dept 62 à 976/Fichier des parcelles (situation 2022)-dpts 62 à 976/*.txt",
        recursive=True)])
    df_parc = pd.concat([df_parc1, df_parc2], axis=0)

    # get rid of unknown SIREN
    parcelles = df_parc.dropna(subset='N° SIREN (Propriétaire(s) parcelle)')
    parcelles = parcelles[~parcelles['N° SIREN (Propriétaire(s) parcelle)'].str.startswith('U', na=False)]
    parcelles = parcelles[['Département (Champ géographique)', 'Nom Commune (Champ géographique)',
                           'Préfixe (Références cadastrales)', 'Section (Références cadastrales)',
                           'N° plan (Références cadastrales)', 'N° de voirie (Adresse parcelle)',
                           'Code voie MAJIC (Adresse parcelle)',
                           'Code voie rivoli (Adresse parcelle)', 'Nature voie (Adresse parcelle)',
                           'Nom voie (Adresse parcelle)',
                           'Code droit (Propriétaire(s) parcelle)',
                           'N° MAJIC (Propriétaire(s) parcelle)',
                           'N° SIREN (Propriétaire(s) parcelle)',
                           'Groupe personne (Propriétaire(s) parcelle)',
                           'Forme juridique (Propriétaire(s) parcelle)',
                           'Dénomination (Propriétaire(s) parcelle)']]

    parcelles = parcelles.drop_duplicates()
    return parcelles
