# SolrNightlyBenchmarks  [![Travis](https://img.shields.io/travis/rust-lang/rust.svg?style=plastic)]()

A comprehensive Solr performance benchmark framework.

## Requirements

      * Java Version: 1.8.0_131 and above
      * Linux OS
      * Apache Maven 3.0.5 and above
      * git version 2.11.1 and above

## Steps to run

     1 git clone https://github.com/viveknarang/lucene-solr.git
     2 git checkout 'SolrNightlyBenchmarks'
     3 cd /dev-tools/
     4 cp -r SolrNightlyBenchmarks to target location on your server
     5 Modify config.properties - point to data files correctly on your server, point your webapp directory to your apache home.
     6 pwd to check that you are in SolrNightlyBenchmarks folder. 
     7 mvn clean compile assembly:single
     8 java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar -ProcessLatestCommit true -Housekeeping true 

      
