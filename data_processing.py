import json
import requests
import pandas as pd
from pymongo import MongoClient

def extract_and_store_data(station_id, filename):
    # URL de l'API avec l'ID de la station spécifique
    url = f"https://airqino-api.magentalab.it/v3/getStationHourlyAvg/{station_id}"

    # Envoyer une requête HTTP GET à l'URL et sauvegarder la réponse
    response = requests.get(url)

    # Charger les données JSON à partir de la réponse
    print("Téléchargement du fichier")
    data = json.loads(response.text)
    data = data["data"]

    # Sauvegarder les données dans un fichier JSON
    print("Écriture dans le fichier JSON")
    with open(filename, 'w') as f:
        json.dump(data, f)

def transform_data(filename):
    # Charger les données JSON à partir du fichier
    with open(filename, 'r') as f:
        data = json.load(f)

    # Créer un DataFrame à partir des données
    df = pd.DataFrame(data)

    # Convertir la colonne 'timestamp' en type datetime
    df['date'] = pd.to_datetime(df['timestamp']).dt.date

    # Sélectionner uniquement les colonnes numériques pour le calcul de la moyenne
    numeric_cols = df.select_dtypes(include='number').columns
    df_daily = df.groupby('date')[numeric_cols].mean()

    # Ajouter des colonnes pour les moyennes CO et PM2.5
    df_daily['CO_moyen'] = df_daily['CO']
    df_daily['PM2.5_moyen'] = df_daily['PM2.5']

    # Ajouter la colonne 'date' au DataFrame final
    df_daily['date'] = pd.to_datetime(df_daily.index)

    # Reorganiser les colonnes pour avoir 'date' en premier
    result_df = df_daily[['date', 'CO_moyen', 'PM2.5_moyen']]

    return result_df

def load_data_into_mongo(df, collection_name, database_name='airflow', mongo_uri='mongodb://localhost:27017'):
    # Connexion à MongoDB
    mongo_client = MongoClient(mongo_uri)

    # Sélection de la base de données
    db = mongo_client[database_name]

    # Sélection de la collection
    collection = db[collection_name]

    # Ajouter la colonne 'date' dans les données
    df['date'] = df.index.astype(str)

    # Convertir le DataFrame en un dictionnaire JSON
    data_json = json.loads(df.to_json(orient='records'))

    # Insérer les données dans la collection MongoDB
    collection.insert_many(data_json)