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

run pyclient: 

```
docker run -d --name pyclient $BOONTADATA_DOCKER_REGISTRY/boontadata-paas/pyclient:0.1
docker exec -ti pyclient bash
docker rm -f pyclient
```
