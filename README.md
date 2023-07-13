# OpenDataWrangler

Ce repo vise à faciliter la manipulation et le prétraitement des ensembles de données open source provenant des institutions publiques françaises retrouveables via data.gouv.com.
Il fournit un ensemble d'outils qui simplifient le processus de travail avec ces tables : le nettoyage des données, la transformation des données et la préparation des données afin de s'assurer que les ensembles de données ouvertes sont structurés et utilisables.

# Organisation

Ce repo est suvibisé en 2 parties :
- **utils** : un ensemble d'outils pour faciliter la manipulation des données
- **parsers** : un ensemble de scripts pour préparer les données des différentes sources
- **exemples** : différents scripts exemples d'utilisation des parsers.

Chaque table open data est accompagnée d'un script de parsing qui permet de préparer les données pour l'analyse. Les scripts de parsing sont écrits en Python et utilisent les outils de la partie utils.
Le fichier exemple permet de voir différents utilisations des parsers.

# Présentation des tables open data

[**SIRENE**](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)
- Base des unités légales
- Base des établissements

[**MAJIC (propriétés des personnes morales)**](https://www.data.gouv.fr/fr/datasets/fichiers-des-locaux-et-des-parcelles-des-personnes-morales/)
- Fichiers des propriétés des parcelles (non-bâtis)
- Fichiers des propriétés des locaux (bâtis)

[**BODACC (Bulletin officiel des annonces civiles et commerciales)**](https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/api/?sort=dateparution)
- Procédures collectives (redressement, liquidation, sauvegarde)
- Procédures de ventes & cessions
- Procédures de radiation

[**INPI (Insititut National de la Propriété Intellectuelle)**](https://data.inpi.fr/content/editorial/Acces_API_Entreprises)
- Données issues des recherches entreprises de l'API

[**BDNB (Base de données nationale des bâtiments)**](https://www.data.gouv.fr/fr/datasets/base-de-donnees-nationale-des-batiments/)
- cartographie du parc de bâtiments existants

[**RNIC (Registre national d'Immatriculation des Copropriétés)**](https://www.data.gouv.fr/fr/datasets/registre-national-dimmatriculation-des-coproprietes/)
- Copropriétés à usage d’habitat

