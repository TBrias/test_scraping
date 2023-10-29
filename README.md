# Exercice Data Engineer

## 1. Installation

* Prérequis: Avoir installé Docker
* Commandes de lancement:
  * Création des images:
    * ````docker-compose build````
  * Démarrage des conteneurs et exécution des services
    * ````docker-compose up````

<br>

Une fois démarré, le script ````main.py```` est joué, il va:
*   Créer un index Elasticsearch
*   Scrapping des données
*   Insertion des données dans Elasticsearch

## 2. Explications des fichiers

### 1. Scrapping

Le librairie scrapy a été utilisée

* [legifrance_spider.py](etl\scrapy_script\predictice_scrapy\spiders\legifrance_spider.py)
    
    Le script récupère la liste des liens présente sur la première page, les lit, puis passera à la page suivante.
    Utiliser pour les dates entre le 01/06/2022 et 30/06/2022

    Pour le debug, en ajoutant des dates de début/fin en arguments au lancement du script, on peut personnaliser la plage de date

    Ex de lancement: scrapy crawl legifrance_spider -a start_date="05/06/2023" -a end_date="06/06/2023"

    Les fichiers .parquet seront écrits dans le répertoire output sous la forme: "date_du_jour.parquet"

* [correspondance_prenoms.py](etl\scrapy_script\predictice_scrapy\spiders\correspondance_prenoms.py)

    Dictionnaire servant à la correspondance des prénoms pour désanonymiser

* [settings.py](etl\scrapy_script\predictice_scrapy\settings.py)

    Ajout de la variable dans le fichier settings de scrapy: USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36", qui sert à accéder au site legifrance

    J'ai également ajouté AUTOTHROTTLE_ENABLED = True, sans ça certaines pages n'étaient pas lues pendant le crawl. Par contre les performances sont impactées

### 2. Elasticsearch
* [insertion.py](etl\insertion.py)

    Script d'insertion des données dans Elasticsearch

    * Lecture du fichier .parquet du jour
    * Transformation du format du champ date
    * Insertion avec la bulk api dans l'index ES

* [create_es_index.py](etl\es\create_es_index.py)

    Script de création de l'index Elasticsearch

    Utilisation du mapping fourni

* [es_mapping.json](etl\es\es_mapping.json)

    Mapping des données de l'index

    Comme c'est un environnement de test, j'ai ajouté "number_of_replicas": 0 en settings, car le nombre de replicas doit être inférieur au nombre de noeud du cluster, sinon l'état reste à 'Yellow'

### 3. Docker
* [Dockerfile](etl\Dockerfile)

    Création des images docker
    
    Installation des dépendances avec le fichier requirements.txt, et mise à jour

    Lancement du script [main.py](etl\main.py)

* [docker-compose.yml](docker-compose.yml)

    Exécution des images Docker en tant que conteneurs

    Téléchargement de l'image Elasticsearch et mise en place du cluster
    

* [Dockerfile](etl\Dockerfile)

    Script d'insertion des données dans Elasticsearch

### 4. Unit tests
* [Dossier de tests unitaires](etl\unit_tests)

    Deux fichiers de TU sur l'insertion et le spider ont été ajoutés
