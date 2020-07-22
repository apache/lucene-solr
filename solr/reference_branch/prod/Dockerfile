FROM ubuntu:20.04

ENV BUILD_DIR=/build/solr
ENV INSTALL_DIR=/opt/solr

RUN mkdir -p ${INSTALL_DIR}
RUN mkdir -p ${BUILD_DIR}

ADD start-solr.sh /start-solr.sh

RUN chmod +x /start-solr.sh

RUN apt-get -y update; apt-get -y upgrade; apt-get -y install openjdk-11-jdk; apt-get -y install ant; apt-get -y install git

RUN cd "${BUILD_DIR}"; git clone https://github.com/apache/lucene-solr.git --branch reference_impl --single-branch reference_impl; \
cd "${BUILD_DIR}/reference_impl"; ant ivy-bootstrap;cd solr; ant package -Dversion=9.0.0-miller_ref_impl; \
cp -r build/*miller_ref_impl/* /opt/solr; chmod +x /opt/solr/bin/solr; \
rm -r "${BUILD_DIR}"; rm -r /root/.ivy2

WORKDIR ${INSTALL_DIR}

ENV PATH=$PATH:/opt/solr/bin


#ENTRYPOINT "/bin/bash"
ENTRYPOINT "/start-solr.sh"