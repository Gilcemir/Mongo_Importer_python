import json
from bson import Binary
import uuid

from pymongo import MongoClient

import mongo


def load_config():
    with open('config.json') as json_file:
        return json.load(json_file)


config = load_config()

source_client = mongo.get_client(config['sourceDB']['connectionString'])
destination_client = mongo.get_client(config['destinationDB']['connectionString'])

databases = config['databases']
##ADICIONAR O PREDICADO AQUI!!
## "ClientId": Binary.from_uuid(uuid.UUID("60da017d-a0e5-b5d9-191c-3a0142867d1b"))
## EXEMPLO DE PREDICADO USANDO BINÁRIO

predicate = {}

for database in databases:
    source_db = source_client[database['name']]
    destination_db = destination_client[database['name']]

    for collection_name in database['collections']:
        source_collection = source_db[collection_name]
        destination_collection = destination_db[collection_name]

        # Verifica se a coleção de destino existe
        if collection_name not in destination_db.list_collection_names():
            print(f"Coleção '{collection_name}' não encontrada no banco de destino, criando automaticamente.")

        documents = source_collection.find(predicate)
        documents_to_insert = list(documents)  # Convert cursor to list

        if documents_to_insert:  # Ensure there are documents to insert
            destination_collection.insert_many(documents_to_insert)
            print(
                f"Copiado {len(documents_to_insert)} documentos da coleção '{collection_name}' no banco '{database['name']}'")

source_client.close()
destination_client.close()
