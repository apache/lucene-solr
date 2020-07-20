FROM openjdk:11


ENV INSTALL_DIR=/opt/solr

RUN mkdir -p ${INSTALL_DIR}

ADD start-solr.sh /start-solr.sh

RUN chmod +x /start-solr.sh


WORKDIR ${INSTALL_DIR}

ENV PATH=$PATH:/opt/solr/miller_ref_impl/bin


#ENTRYPOINT "/bin/bash"
ENTRYPOINT "/start-solr.sh"