FROM openjdk:11-jdk-slim

ENV BUILD_DIR=/build/solr
ENV INSTALL_DIR=/opt/solr
ENV ANT_INSTALL_DIR=/opt/ant
ENV ANT_TGZ_FILE="apache-ant-1.9.15"

RUN mkdir -p ${INSTALL_DIR}
RUN mkdir -p ${BUILD_DIR}

ADD start-solr.sh /start-solr.sh
ADD start-solr.sh /start-solr.sh

RUN chmod +x /start-solr.sh

RUN apt-get -y update; apt-get -y upgrade; apt-get -y install git;apt-get -y install wget; mkdir -p "${ANT_INSTALL_DIR}"; \
wget http://mirror.cc.columbia.edu/pub/software/apache/ant/binaries/${ANT_TGZ_FILE}-bin.tar.gz; \
tar -xzvf ${ANT_TGZ_FILE}-bin.tar.gz;cp -r ${ANT_TGZ_FILE}/* /opt/ant

RUN cd "${BUILD_DIR}"; git clone https://github.com/apache/lucene-solr.git --branch reference_impl --single-branch reference_impl; \
cd "${BUILD_DIR}/reference_impl"; /opt/ant/bin/ant ivy-bootstrap;cd solr; /opt/ant/bin/ant package -Dversion=9.0.0-miller_ref_impl; \
cp -r build/*miller_ref_impl/* /opt/solr; chmod +x /opt/solr/bin/solr; \
rm -r "${BUILD_DIR}"; rm -r /root/.ivy2; apt-get -y remove ant;\
apt-get -y remove git; apt -y autoclean; apt -y autoremove

WORKDIR ${INSTALL_DIR}

ENV PATH=$PATH:/opt/solr/bin:/opt/ant/bin


ENTRYPOINT "/bin/bash"
#ENTRYPOINT "/start-solr.sh"