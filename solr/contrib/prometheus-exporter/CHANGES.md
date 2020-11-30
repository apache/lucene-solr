This file lists release notes for this module.
Prior to version 9, changes were in Solr's CHANGES.txt

9.0.0
======================

Improvements
----------------------
* SOLR-14972: Change default port of prometheus exporter to 8989 
  because it clashed with default embedded zookeeper port (janhoy)

Other Changes
----------------------
* SOLR-14915: Reduced dependencies from Solr server down to just SolrJ.  Don't add WEB-INF/lib.
  * Can run via gradle, "gradlew run"
  * Has own log4j2.xml now
  * Was missing some dependencies in lib/; now has all except SolrJ & logging.
  (David Smiley, Houston Putman)

* SOLR-14957: Add Prometheus Exporter to docker PATH. Fix classpath issues. (Houston Putman)
