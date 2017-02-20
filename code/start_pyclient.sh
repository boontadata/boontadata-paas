#!/bin/bash

docker rm -f pyclient
docker run -d --name pyclient \
    --env BOONTADATA_PAAS_iothub_registryrw_connectionstring=$BOONTADATA_PAAS_iothub_registryrw_connectionstring \
    --env BOONTADATA_PAAS_docdb_host=$BOONTADATA_PAAS_docdb_host \
    --env BOONTADATA_PAAS_docdb_key=$BOONTADATA_PAAS_docdb_key \
    --env BOONTADATA_PAAS_docdb_dbname=$BOONTADATA_PAAS_docdb_dbname \
    --env BOONTADATA_PAAS_docdb_collectionname=$BOONTADATA_PAAS_docdb_collectionname \
    $BOONTADATA_DOCKER_REGISTRY/boontadata-paas/pyclient:0.1

