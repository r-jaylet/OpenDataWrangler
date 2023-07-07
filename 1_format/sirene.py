import pandas as pd
from datetime import datetime
import numpy as np


def search_unite_legale(siren, StockUniteLegale, StockUniteLegaleHistorique):
    # get col names
    StockUniteLegale_col = pd.read_csv('../0_data/base_sirene/StockUniteLegale_utf8.csv', nrows=3)
    StockUniteLegaleHistorique_col = pd.read_csv('../0_data/base_sirene/StockUniteLegaleHistorique_utf8.csv', nrows=3)

    # extract concerned rows id
    UL = StockUniteLegale[StockUniteLegale.siren == siren]
    ULH = StockUniteLegaleHistorique[StockUniteLegaleHistorique.siren == siren]

    # search for full data
    info_siren = pd.read_csv('../0_data/base_sirene/StockUniteLegale_utf8.csv', skiprows=UL.index[0], nrows=1)
    info_siren_historique = pd.read_csv('../0_data/base_sirene/StockUniteLegaleHistorique_utf8.csv',
                                        skiprows=ULH.index[0], nrows=len(ULH))

    # reformat data
    info_siren = info_siren.set_axis(StockUniteLegale_col.columns, axis=1)
    info_siren_historique = info_siren_historique.set_axis(StockUniteLegaleHistorique_col.columns, axis=1)

    info_siren = info_siren[['siren', 'dateCreationUniteLegale', 'nombrePeriodesUniteLegale', 'dateDebut',
                             'etatAdministratifUniteLegale', 'denominationUniteLegale', 'nicSiegeUniteLegale']]

    info_siren_historique = info_siren_historique[['siren', 'dateFin', 'dateDebut', 'etatAdministratifUniteLegale',
                                                   'changementEtatAdministratifUniteLegale', 'denominationUniteLegale',
                                                   'changementDenominationUniteLegale',
                                                   'changementCategorieJuridiqueUniteLegale',
                                                   'changementActivitePrincipaleUniteLegale',
                                                   'nicSiegeUniteLegale', 'changementNicSiegeUniteLegale']]

    return info_siren, info_siren_historique


def search_etablissement(siren, StockEtablissement, StockEtablissementHistorique, StockEtablissementLiensSuccession):
    # get col names
    StockEtablissement_col = pd.read_csv('../0_data/base_sirene/StockEtablissement_utf8.csv', nrows=3)
    StockEtablissementHistorique_col = pd.read_csv('../0_data/base_sirene/StockEtablissementHistorique_utf8.csv',
                                                   nrows=3)

    # extract concerned rows id
    E = StockEtablissement[StockEtablissement.siren == siren]
    EH = StockEtablissementHistorique[StockEtablissementHistorique.siren == siren]

    # search for full data
    info_siret = pd.read_csv('../0_data/base_sirene/StockEtablissement_utf8.csv', skiprows=E.index[0], nrows=len(E))
    info_siret_historique = pd.read_csv('../0_data/base_sirene/StockEtablissementHistorique_utf8.csv',
                                        skiprows=EH.index[0], nrows=len(EH))

    # reformat data
    info_siret = info_siret.set_axis(StockEtablissement_col.columns, axis=1)
    info_siret_historique = info_siret_historique.set_axis(StockEtablissementHistorique_col.columns, axis=1)

    # fetch succession
    list_siret = list(info_siret_historique.siret.unique())
    info_succession = StockEtablissementLiensSuccession.loc[
        (StockEtablissementLiensSuccession.siretEtablissementSuccesseur.isin(list_siret)) | (
            StockEtablissementLiensSuccession.siretEtablissementPredecesseur.isin(list_siret))]

    info_siret = info_siret[['siren', 'nic', 'siret', 'dateCreationEtablissement', 'etablissementSiege',
                             'nombrePeriodesEtablissement', 'numeroVoieEtablissement', 'typeVoieEtablissement',
                             'libelleVoieEtablissement', 'codePostalEtablissement',
                             'libelleCommuneEtablissement', 'codeCommuneEtablissement',
                             'dateDebut', 'etatAdministratifEtablissement']]

    info_siret_historique = info_siret_historique[['siren', 'nic', 'siret', 'dateFin', 'dateDebut',
                                                   'etatAdministratifEtablissement',
                                                   'changementEtatAdministratifEtablissement',
                                                   'changementActivitePrincipaleEtablissement']]

    return info_siret, info_siret_historique, info_succession


def list_etablissements(info_siret, info_siret_historique):
    etablissements = info_siret.copy().drop(['dateDebut'], axis=1)

    etablissements['address'] = etablissements[['numeroVoieEtablissement', 'typeVoieEtablissement',
                                                'libelleVoieEtablissement',
                                                'libelleCommuneEtablissement']].astype(str).agg(' '.join, axis=1)

    # get dateDebut
    etablissements_hist = info_siret_historique.copy()
    etablissements_hist['dateDebut'] = pd.to_datetime(etablissements_hist['dateDebut'])
    etablissements_hist = \
    etablissements_hist.sort_values('dateDebut', ascending=True).drop_duplicates('siret').sort_index()[
        ['siret', 'dateDebut']]
    etablissements = etablissements.merge(etablissements_hist, left_on='siret', right_on='siret', how='left')
    etablissements = etablissements[
        ['siren', 'siret', 'etablissementSiege', 'address', 'dateDebut', 'etatAdministratifEtablissement']]
    etablissements['dateDebut'] = pd.to_datetime(etablissements['dateDebut'])
    etablissements = etablissements.replace(datetime.strptime('1900-01-01', '%Y-%m-%d'), np.NaN)

    etablissements_actifs = etablissements[etablissements.etatAdministratifEtablissement == 'A'].drop(
        'etatAdministratifEtablissement', axis=1)
    etablissements_fermes = etablissements[etablissements.etatAdministratifEtablissement == 'F'].drop(
        'etatAdministratifEtablissement', axis=1)

    # get dateFin for closed buildings
    etablissements_hist = info_siret_historique.copy()
    etablissements_hist['dateDebut'] = pd.to_datetime(etablissements_hist['dateDebut'])
    etablissements_hist = \
    etablissements_hist.sort_values('dateDebut', ascending=False).drop_duplicates('siret').sort_index()[
        ['siret', 'dateDebut']]
    etablissements_hist = etablissements_hist.rename({'dateDebut': 'dateFin'}, axis=1)
    etablissements_fermes = etablissements_fermes.merge(etablissements_hist, left_on='siret', right_on='siret',
                                                        how='left')
    etablissements_fermes['dateFin'] = pd.to_datetime(etablissements_fermes['dateFin'])

    # compute elapsed time
    etablissements_actifs['time spent (in y)'] = round(
        (datetime.now() - etablissements_actifs['dateDebut']).dt.days / 365, 2)
    etablissements_fermes['time spent (in y)'] = round(
        (etablissements_fermes['dateFin'] - etablissements_fermes['dateDebut']).dt.days / 365, 2)

    return etablissements_actifs.sort_values(by='dateDebut', ascending=False), etablissements_fermes.sort_values(
        by='dateDebut', ascending=False)