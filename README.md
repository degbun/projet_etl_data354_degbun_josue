# projet_etl_data354_degbun_josue
Le projet vise à construire un ETL (Extract, Transform, Load) pour l'extraction et le traitement des données relatives à la qualité de l'air provenant de l'API AirQuino.
## Structure du projet
1. `Images/`:
    Dossier contenant 5 images liées au Dashboard sur superset pour la visualisation
   - image1.jpg: Montre l'evolution du CO_moyen vs PM2.5 moyen dans la staion1 et la station2
   - image2.jpg: Elle fait une comparaison de l'évolution du CO_moyen de la station1 vs station2 puis l'évolution de PM2.5 moyen station1 vs station2
   - image3.jpg: Montre la valeur max de CO_oyen de le station1 vs station2 puis la valeur max PM2.5 moyen de la station1 vs station2
   - image4.jpg: Montre le waterfall de CO_moyen et PM2.5 moyen de la station1
   - image5.jpg: Montre le waterfall de CO_moyen et PM2.5 moyen de la station2
2.  `airflow.station1.csv`: le fichier csv aggrégées des données de la station1

3. `airflow.station2.csv`  :  le fichier csv aggrégées des données de la station2

4. `data_processing.py` : fournit un ensemble de fonctions pour l'extraction, la transformation et le chargement (ETL) de données. Ces fonctions seront utilisées par le processus ETL principal (etl.py)

5.  `etl.py`:Le script etl.py coordonne l'extraction, la transformation et le chargement (ETL) des données provenant de deux stations spécifiques en appelant les fonctions définies dans data_processing.py, puis stocke les résultats dans deux collections distinctes de MongoDB.
6.   `station1_data.json` : données sous forme json de la station1

7. `station2_data.json` : données sous forme json de la station2

## Installation
Avant de procéder à l'installation, assurez-vous d'être dans le répertoire du projet. Utilisez la commande suivante pour installer les dépendances nécessaires à l'aide du fichier requirements.txt :
```bash
pip install -r requirements.txt
```
## Exécution du script ETL
### Configuration de la Base de Données MongoDB
Avant d'exécuter le script ETL, assurez-vous de configurer correctement votre base de données MongoDB. Suivez ces étapes pour la configuration :

#### Étape 1: Installation de MongoDB
Assurez-vous que MongoDB est installé sur votre machine. Vous pouvez le télécharger à partir du site officiel de MongoDB sur: [Téléchargement MongoDB](https://www.mongodb.com/try/download/community)

#### Étape 2: Démarrage de MongoDB
Démarrez le serveur MongoDB sur le port 27017 de votre machine. Vous pouvez utiliser la commande suivante :
```bash
mongod --port 27017
```
Ou encore utiliser MongoDB Compass pour lancer un noeud Mongo. Télécharger MongoDB Compass sur :[Téléchargement MongoDB Compass](https://www.mongodb.com/try/download/shell)

#### Étape 3: Création des Collections
Ouvrez votre terminal MongoDB et assurez-vous que votre base de données "airflow" existe. Si elle n'existe pas, vous pouvez la créer en utilisant la commande suivante :
```bash
use airflow
```
Ensuite, créez les collections "station1" et "station2" dans la base de données "airflow".
```bash
db.createCollection("station1")
```
```bash
db.createCollection("station2")
```
#### Étape 4: Exécution du Script ETL
Maintenant, vous pouvez exécuter le script ETL pour extraire, transformer et charger les données dans les collections "station1" et "station2" de la base de données "airflow". Utilisez la commande suivante :
```bash
python etl.py
