# How to

## set variables

this could be added to your ~/.bashrc file 

The following values are an example, please change with your own values

```
export BOONTADATA_PAAS=$HOME/boontadata-paas
export BOONTADATA_PAAS_iothub_registryrw_connectionstring="HostName=bd34iothub.azure-devices.net;SharedAccessKeyName=registryReadWrite;SharedAccessKey=59NoN###obfuscated###hjeDk="
export BOONTADATA_PAAS_docdb_host="https://mydocumentdb.documents.azure.com:443/"
export BOONTADATA_PAAS_docdb_key="exf###obfuscated###NIuw=="
export BOONTADATA_PAAS_docdb_dbname="mydocdb"
export BOONTADATA_PAAS_docdb_collectionname="mycoll"
```

## to be integrated in bash scripts

### run scenario

```
cd $BOONTADATA_PAAS/code
. start_pyclient.sh
docker exec -ti pyclient python truncate_docdb.py
docker exec -ti pyclient python inject.py
docker exec -ti pyclient python compare.py
docker rm -f pyclient
```
