from data_processing import extract_and_store_data, transform_data, load_data_into_mongo

def main():
    # Spécifier les ID de station et les noms de fichier pour chaque station
    station1_id = 283164601
    station1_filename = 'station1_data.json'
    station2_id = 283181971
    station2_filename = 'station2_data.json'

    # Extraire et stocker les données pour la première station
    extract_and_store_data(station1_id, station1_filename)

    # Extraire et stocker les données pour la deuxième station
    extract_and_store_data(station2_id, station2_filename)

    # Transformer les données pour chaque station
    df_station1 = transform_data(station1_filename)
    df_station2 = transform_data(station2_filename)

    # Charger les données dans MongoDB pour chaque station
    load_data_into_mongo(df_station1, 'station1')
    load_data_into_mongo(df_station2, 'station2')

if __name__ == "__main__":
    main()
