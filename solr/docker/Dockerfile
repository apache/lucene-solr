ARG SOLR_PACKAGE_IMAGE
ARG BASE_IMAGE=openjdk:11-jre-slim

FROM $SOLR_PACKAGE_IMAGE as solr_package

FROM $BASE_IMAGE as runtime

LABEL maintainer="The Apache Lucene/Solr Project"
LABEL repository="https://github.com/apache/lucene-solr"

# Override the default github URL to provide a mirror for github releases.
ARG GITHUB_URL=github.com

RUN set -ex; \
    apt-get update; \
    apt-get -y install acl dirmngr gpg lsof procps wget netcat gosu tini; \
    rm -rf /var/lib/apt/lists/*; \
    cd /usr/local/bin; wget -nv https://${GITHUB_URL}/apangin/jattach/releases/download/v1.5/jattach; chmod 755 jattach; \
    echo >jattach.sha512 "d8eedbb3e192a8596c08efedff99b9acf1075331e1747107c07cdb1718db2abe259ef168109e46bd4cf80d47d43028ff469f95e6ddcbdda4d7ffa73a20e852f9  jattach"; \
    sha512sum -c jattach.sha512; rm jattach.sha512

ENV SOLR_USER="solr" \
    SOLR_UID="8983" \
    SOLR_GROUP="solr" \
    SOLR_GID="8983" \
    PATH="/opt/solr/bin:/opt/docker-solr/scripts:/opt/solr/contrib/prometheus-exporter/bin:$PATH" \
    SOLR_INCLUDE=/etc/default/solr.in.sh \
    SOLR_HOME=/var/solr/data \
    SOLR_PID_DIR=/var/solr \
    SOLR_LOGS_DIR=/var/solr/logs \
    LOG4J_PROPS=/var/solr/log4j2.xml \
    SOLR_JETTY_HOST="0.0.0.0"

RUN set -ex; \
  groupadd -r --gid "$SOLR_GID" "$SOLR_GROUP"; \
  useradd -r --uid "$SOLR_UID" --gid "$SOLR_GID" "$SOLR_USER"

COPY --chown=0:0 scripts /opt/docker-solr/scripts

ARG SOLR_VERSION

COPY --from=solr_package "/opt/solr-$SOLR_VERSION.tgz" "/opt/solr-$SOLR_VERSION.tgz"

RUN set -ex; \
  tar -C /opt --extract --file "/opt/solr-$SOLR_VERSION.tgz" && \
  rm "/opt/solr-$SOLR_VERSION.tgz"; \
  (cd /opt; ln -s "solr-$SOLR_VERSION" solr); \
  rm -Rf /opt/solr/docs/ /opt/solr/dist/{solr-solrj-$SOLR_VERSION.jar,solrj-lib,solr-test-framework-$SOLR_VERSION.jar,test-framework}; \
  mkdir -p /opt/solr/server/solr/lib /docker-entrypoint-initdb.d /opt/docker-solr; \
  chown -R 0:0 "/opt/solr-$SOLR_VERSION"; \
  find "/opt/solr-$SOLR_VERSION" -type d -print0 | xargs -0 chmod 0755; \
  find "/opt/solr-$SOLR_VERSION" -type f -print0 | xargs -0 chmod 0644; \
  chmod -R 0755 "/opt/solr-$SOLR_VERSION/bin" "/opt/solr-$SOLR_VERSION/contrib/prometheus-exporter/bin/solr-exporter" /opt/solr-$SOLR_VERSION/server/scripts/cloud-scripts; \
  cp /opt/solr/bin/solr.in.sh /etc/default/solr.in.sh; \
  mv /opt/solr/bin/solr.in.sh /opt/solr/bin/solr.in.sh.orig; \
  mv /opt/solr/bin/solr.in.cmd /opt/solr/bin/solr.in.cmd.orig; \
  chown root:0 /etc/default/solr.in.sh; \
  chmod 0664 /etc/default/solr.in.sh; \
  mkdir -p /var/solr/data /var/solr/logs; \
  (cd /opt/solr/server/solr; cp solr.xml zoo.cfg /var/solr/data/); \
  cp /opt/solr/server/resources/log4j2.xml /var/solr/log4j2.xml; \
  find /var/solr -type d -print0 | xargs -0 chmod 0770; \
  find /var/solr -type f -print0 | xargs -0 chmod 0660; \
  sed -i -e "s/\"\$(whoami)\" == \"root\"/\$(id -u) == 0/" /opt/solr/bin/solr; \
  sed -i -e 's/lsof -PniTCP:/lsof -t -PniTCP:/' /opt/solr/bin/solr; \
  chown -R "0:0" /opt/solr-$SOLR_VERSION /docker-entrypoint-initdb.d /opt/docker-solr; \
  chown -R "$SOLR_USER:0" /var/solr;

VOLUME /var/solr
EXPOSE 8983
WORKDIR /opt/solr
USER $SOLR_USER

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["solr-foreground"]
