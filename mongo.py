import pymongo


# get mongo client
def get_client(connection_string):
    client = pymongo.MongoClient(connection_string)
    print("Connect Successful to mongodb")
    return client


# Close mongo db connection
def close_connection(client):
    print("Connection closed with mongo Client")
    client.close()
