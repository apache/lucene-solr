# SolrNightlyBenchmarks  [![Travis](https://img.shields.io/travis/rust-lang/rust.svg?style=plastic)]()

A comprehensive Solr performance benchmark framework.

## Requirements

      * Java Version: 1.8.0_131 and above
      * Linux OS
      * Apache Maven 3.0.5 and above
      * git version 2.11.1 and above
      * Apache HTTP webserver (If you want this framwork's output to be accessible over a network through a browser)

## Steps to run

     1. git clone https://github.com/viveknarang/lucene-solr.git
     2. git checkout 'SolrNightlyBenchmarks'
     3. cd /dev-tools/
     4. cp -r SolrNightlyBenchmarks to target location on your server
     5. Modify config.properties - point to data files correctly on your server, point your webapp directory to your apache home.
     6. pwd to check that you are in SolrNightlyBenchmarks folder. 
     7. mvn clean compile assembly:single
     8. java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar -ProcessLatestCommit true -Housekeeping true 
     
## Possible parameters

     * -RunSilently true                       Use this if you do not want any output on console.
     * -ProcessLatestCommit true               Use this parameter if you want the system to look for the latest commit to work on.
     * -ProcessWithCommitID XXXXXXXXXXXX       Use this parameter if you want the system to use the commit hash to work on.
     
     * -ProcessCommitsFromQueue true           Use this parameter if you want the system to work on the commit hash present in the queue.
     * -RegisterLatestCommit true              This parameter is used in conjunction with the last parameter. 
     
## A commit queue sub-utility

     There is a light-weight utility in this framework which is used to look for commits in a specified time periods. 
     
     

     
      
