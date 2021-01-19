#!/bin/bash

set -x

hostip=$(ip route show | awk '/default/ {print $3}')
echo "host: $hostip"

if [ ! -d "/opt/solr/reference_impl" ]; then
  apt-get -y update
  apt-get -y upgrade
  apt-get -y install ant

  git clone https://github.com/apache/lucene-solr.git --branch reference_impl --single-branch reference_impl
  cd reference_impl
  ant ivy-bootstrap
  cd solr
  ant package -Dversion=9.0.0-miller_ref_impl
  cp build/*miller_ref_impl/* /opt/solr
  chmod +x /opt/solr/bin/solr
fi


bash /opt/solr/reference_impl/solr/bin/solr start -c -m 1g -z "172.19.0.2:2181" -p ${SOLR_PORT:-9998} -force -f

