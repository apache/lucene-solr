#!/bin/bash

set -x

hostip=$(ip route show | awk '/default/ {print $3}')
echo "host: $hostip"


bash /opt/solr/bin/solr start -c -m "${SOLR_HEAP:-1g}" -z "${ZK_ADDRESS:-zookeeper:2181}" -p ${SOLR_PORT:-9998} -force -f

