import pandas as pd

def bodacc_format():

    df_bodacc = pd.read_csv('0_data/annonces-commerciales.csv', usecols=['id', 'dateparution', 'numeroannonce', 'jugement',
                                                                  'numerodepartement', 'region_nom_officiel',
                                                                  'tribunal', 'commercant', 'ville',
                                                                  'registre'], sep=';')

    # get jugement type
    df_bodacc['nature_jugement'] = df_bodacc['jugement'].apply(lambda x: json.loads(x)['nature'])
    df_bodacc = df_bodacc.drop('jugement', axis=1)

    # get rid of null siren with no info
    df_bodacc = df_bodacc.dropna(subset='registre')
    df_bodacc['registre'] = df_bodacc['registre'].astype(str)
    df_bodacc['registre'] = df_bodacc['registre'].apply(lambda x : x.replace('000 000 000,000000000,', ''))
    df_bodacc = df_bodacc[df_bodacc['registre'] != '000 000 000,000000000']

    # aggregated rows ??? mixed commercant name and registre witgh no link
    len_reg = df_bodacc['registre'].apply(lambda x : len(x))
    df_bodacc[df_bodacc.index.isin(list(len_reg[len_reg != 21].index))].tail(3)

    # extract SIREN
    df_bodacc['SIREN'] = df_bodacc['registre'].apply(lambda x: x.split(',')[0].replace(' ', ''))
    df_bodacc = df_bodacc.drop('registre', axis=1)

    # Keep last annonce of each SIREN
    df_bodacc = df_bodacc.drop_duplicates('SIREN', keep='last')

    return df_bodacc