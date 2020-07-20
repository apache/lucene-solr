FROM openjdk:11

ENV BUILD_DIR=/build/solr
ENV INSTALL_DIR=/opt/solr

RUN mkdir -p ${INSTALL_DIR}

ADD start-solr.sh /start-solr.sh

RUN chmod +x /start-solr.sh

RUN apt-get -y update; apt-get -y upgrade; apt-get -y install ant

RUN git clone https://github.com/apache/lucene-solr.git --branch reference_impl --single-branch reference_impl; \
 cd reference_impl;ant ivy-bootstrap;cd solr;ant package -Dversion=9.0.0-miller_ref_impl;

WORKDIR ${BUILD_DIR}

ENV PATH=$PATH:/opt/solr/miller_ref_impl/bin


ENTRYPOINT "/bin/bash"
#ENTRYPOINT "/start-solr.sh"