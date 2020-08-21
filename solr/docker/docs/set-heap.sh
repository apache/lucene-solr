#!/bin/bash
#
# This script is mainly an illustration for the docker-entrypoint-initdb.d extension mechanism.
# Run it with e.g.:
#
#   docker run -d -P -v $PWD/docs/set-heap.sh:/docker-entrypoint-initdb.d/set-heap.sh solr
#
# The SOLR_HEAP configuration technique here is usable for older versions of Solr.
# From Solr 6.3 setting the SOLR_HEAP can be done more easily with:
#
#   docker run -d -P -e SOLR_HEAP=800m docker-solr/docker-solr:6.3.0
#
set -e
cp /opt/solr/bin/solr.in.sh /opt/solr/bin/solr.in.sh.orig
sed -e 's/SOLR_HEAP=".*"/SOLR_HEAP="1024m"/' </opt/solr/bin/solr.in.sh.orig >/opt/solr/bin/solr.in.sh
grep '^SOLR_HEAP=' /opt/solr/bin/solr.in.sh
