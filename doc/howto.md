# How to

## set variables

this could be added to your ~/.bashrc file 

The following values are an example, please change with your own values

```
export BOONTADATA_PAAS=$HOME/boontadata-paas

export BOONTADATA_PAAS-PREFIX=bdp

```

## to be integrated in bash scripts

run pyclient: 

```
docker run -d --name pyclient $BOONTADATA_DOCKER_REGISTRY/boontadata-paas/pyclient:0.1
docker exec -ti pyclient bash
docker rm -f pyclient
```
