#!/bin/bash

set -x

hostip=$(ip route show | awk '/default/ {print $3}')
echo "host: $hostip"


bash /opt/solr/reference_impl/solr/bin/solr start -c -m 1g -z "172.19.0.2:2181" -p ${SOLR_PORT:-9998} -force -f

