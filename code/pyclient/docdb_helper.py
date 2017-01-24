import os
import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors

class DocDbHelper:
    def __init__(self):
        try:
            self.host=os.environ['BOONTADATA_PAAS_docdb_host']
            self.key=os.environ['BOONTADATA_PAAS_docdb_key']
            self.dbname=os.environ['BOONTADATA_PAAS_docdb_dbname']
            self.collectionname=os.environ['BOONTADATA_PAAS_docdb_collectionname']
        except Exception as ex:
            print("please set the following environment variables about DocumentDb")
            print("- BOONTADATA_PAAS_docdb_host           : the host. Example: https://mydocumentdb.documents.azure.com:443/")
            print("- BOONTADATA_PAAS_docdb_key            : the primaryKey or secondary key. Example: exf###obfuscated###NIuw==")
            print("- BOONTADATA_PAAS_docdb_dbname         : the database name. Example: mydocdb")
            print("- BOONTADATA_PAAS_docdb_collectionname : the collection name. Example: collectionname")
            raise ex
        self.collection_link="dbs/" + self.dbname + "/colls/" + self.collectionname
        self.client = document_client.DocumentClient(self.host, {'masterKey': self.key})

    def readfirstdoc(self):
        options = {}
        options['maxItemCount'] = 1
        query = {'query': 'SELECT * from c'}
        result = self.client.QueryDocuments(self.collection_link, query, options)

        return(next((doc for doc in result)))

    def senddata(self, docs):
        for d in docs:
            self.client.CreateDocument(self.collection_link, d)
    
    def truncate(self):
        options = {}
        query = {'query': 'SELECT * from c'}
        result = self.client.QueryDocuments(self.collection_link, query, options)
        for d in result:
            self.client.DeleteDocument(d['_self'])

    def select(self, query):
        options = {}
        query = {'query': query}
        result = self.client.QueryDocuments(self.collection_link, query, options)
        return result