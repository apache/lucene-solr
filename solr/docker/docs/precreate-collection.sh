#!/bin/bash
#
# A script that creates a core by copying config before starting solr.
#
# To use this, map this file into your container's docker-entrypoint-initdb.d directory:
#
#     docker run -d -P -v $PWD/precreate-collection.sh:/docker-entrypoint-initdb.d/precreate-collection.sh solr

CORE=${CORE:-gettingstarted}
if [[ -d "/opt/solr/server/solr/$CORE" ]]; then
    echo "$CORE is already present on disk"
    exit 0
fi

mkdir -p "/opt/solr/server/solr/$CORE/"
cd "/opt/solr/server/solr/$CORE" || exit
touch core.properties
# TODO: we may want a more minimal example here
cp -r /opt/solr/example/files/* .
echo created "/opt/solr/server/solr/$CORE"
