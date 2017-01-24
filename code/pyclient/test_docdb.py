import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors

docdb_masterKey='exfUqiW9hWcw9myxouEFKqjMOfWzlYN1PqWKk7CWemss9JYK9uWLrzIuHE8pWVCwzheCa9l8JIwLXwpgeTNIuw=='
docdb_host='https://bd34documentdb.documents.azure.com:443/'
docdb_databaseid='bd34docdb'
docdb_collectionid='debug'

client = document_client.DocumentClient(
    docdb_host, 
    {'masterKey': docdb_masterKey})

# Read databases and take first since id should not be duplicated.
db = next((data for data in client.ReadDatabases() if data['id'] == docdb_databaseid))
print("database:")
print(db)

# Read collections and take first since id should not be duplicated.
coll = next((coll for coll in client.ReadCollections(db['_self']) if coll['id'] == docdb_collectionid))
print("collection:")
print(coll)

# Read documents and take first since id should not be duplicated.
doc = next((doc for doc in client.ReadDocuments(coll['_self']) if doc['id'] != None))
print("doc:")
print(doc)