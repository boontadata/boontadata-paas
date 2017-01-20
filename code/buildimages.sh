#!/bin/bash

#usage: . buildimages.sh <reset|noreset>
if test $# -lt 1; then reset=noreset; else reset=$1; fi

if test -z $BOONTADATA_DOCKER_REGISTRY
then
    echo BOONTADATA_DOCKER_REGISTRY variable must not be null or empty
    echo you also need to login with `docker login`
    return 1
fi

if test -z $BOONTADATA_PAAS
then
    echo BOONTADATA_PAAS variable must not be null or empty
    return 1
fi

build_and_push()
{
    folderpath=$1
    filepath=$folderpath/Dockerfile
    filepath2=$folderpath/tmpDockerfile
    tagname=$(eval echo "`head -1 $filepath | awk '{print $2}'`")
    tagversion=`head -3 $filepath | tail -1| awk '{print $3}'`
    fulltag="$tagname:$tagversion"

    if test $reset = "reset"
    then
        echo "will reset image $fulltag"
        docker rmi $fulltag
    fi

    imageavailability=`docker images | grep "$tagname *$tagversion"`
    if test -n "$imageavailability"
    then
        echo "local image $fulltag already exists, no reset so no rebuild"
    else
        echo "will build $fulltag"
        if test -e $filepath2; then rm $filepath2; fi
        replacestring="s/\$BOONTADATA_DOCKER_REGISTRY/${BOONTADATA_DOCKER_REGISTRY}/g"
        sed $replacestring $filepath > $filepath2

        docker build -t $fulltag $folderpath --file $filepath2
        echo "local docker images for $tagname:"
        docker images | grep "$tagname"
        docker push $fulltag
    fi
}

#create containers
build_and_push $BOONTADATA_PAAS/code/pyclientbase
build_and_push $BOONTADATA_PAAS/code/pyclient

docker images | grep $BOONTADATA_DOCKER_REGISTRY/boontadata-paas
