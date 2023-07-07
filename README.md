# OpenDataWrangler

Ce repo vise à faciliter la manipulation et le prétraitement des ensembles de données ouvertes provenant des institutions publiques françaises. Il fournit un ensemble d'outils qui simplifient le processus de travail avec ces ensembles de données. Le référentiel se concentre sur des tâches telles que le nettoyage des données, la transformation des données et la préparation des données afin de s'assurer que les ensembles de données ouvertes sont structurés et utilisables.

Ce repo est suvibisé en 2 parties :
- **utils** : un ensemble d'outils pour faciliter la manipulation des données
- **parsers** : un ensemble de scripts pour préparer les données des différentes sources

Chaque table open data est accompagnée d'un script de parsing qui permet de préparer les données pour l'analyse. Les scripts de parsing sont écrits en Python et utilisent les outils de la partie utils.

# Présentation des tables open data

[**SIRENE**](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)
- Base des unités légales
- Base des établissements
- Base des unités légales historisées
- Base des établissements historisées

[**MAJIC (propriétés des personnes morales)**](https://www.data.gouv.fr/fr/datasets/fichiers-des-locaux-et-des-parcelles-des-personnes-morales/)
- Fichiers des propriétés des parcelles (non-bâtis)
- Fichiers des propriétés des locaux (bâtis)

[**BODACC (Bulletin officiel des annonces civiles et commerciales)**](https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/api/?sort=dateparution)
- Procédures collectives (redressement, liquidation, sauvegarde)
- Procédures de ventes & cessions
- Procédures de radiation

[**INPI (Insititut National de la Propriété Intellectuelle)**](https://data.inpi.fr/content/editorial/Acces_API_Entreprises)
- Données issues des recherches entreprises de l'API

[**BDNB (Base de données nationale des bâtiments)**](https://data.inpi.fr/content/editorial/Acces_API_Entreprises](https://www.data.gouv.fr/fr/datasets/base-de-donnees-nationale-des-batiments/)https://www.data.gouv.fr/fr/datasets/base-de-donnees-nationale-des-batiments)
- lipsum

[**RNIC (Registre national d'Immatriculation des Copropriétés)**](https://data.inpi.fr/content/editorial/Acces_API_Entreprises](https://www.data.gouv.fr/fr/datasets/base-de-donnees-nationale-des-batiments/)https://www.data.gouv.fr/fr/datasets/base-de-donnees-nationale-des-batiments/](https://www.data.gouv.fr/fr/datasets/registre-national-dimmatriculation-des-coproprietes/)https://www.data.gouv.fr/fr/datasets/registre-national-dimmatriculation-des-coproprietes/)
- lipsum

