FROM openjdk:11-jdk-slim

ENV BUILD_DIR=/build/solr
ENV INSTALL_DIR=/opt/solr

RUN mkdir -p ${INSTALL_DIR}
RUN mkdir -p ${BUILD_DIR}

RUN apt-get -y update; apt-get -y upgrade; apt-get -y install git

RUN cd "${BUILD_DIR}"; git clone https://github.com/apache/lucene-solr.git --branch reference_impl --single-branch reference_impl; \
cd "${BUILD_DIR}/reference_impl"; ./gradlew -p solr distTar; \
cp -r "${BUILD_DIR}"/reference_impl/solr/packaging/build/distributions/solr-*.tgz /opt/solr; \
rm -rf /root/.m2; rm -rf /root/.ivy2; \
#rm -r "${BUILD_DIR}"; \
apt-get -y remove git; apt -y autoclean; apt -y autoremove

WORKDIR ${INSTALL_DIR}

ENV PATH=$PATH:/opt/solr/bin


ENTRYPOINT "/bin/bash"