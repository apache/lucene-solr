# SolrNightlyBenchmarks  [![Travis branch](https://img.shields.io/travis/rust-lang/rust/master.svg)]()

A comprehensive Solr performance benchmark framework.

## Requirements

      * Java Version: 1.8.0_131 and above
      * Linux OS
      * Apache Maven 3.0.5 and above
      * git version 2.11.1 and above
      * [OPTIONAL] Apache HTTP webserver (If you want this framwork's output to be accessible over a network through a browser)

## Steps to run

     1. git clone https://github.com/viveknarang/lucene-solr.git
     2. git checkout 'SolrNightlyBenchmarks'
     3. cd /dev-tools/
     4. cp -r SolrNightlyBenchmarks to target location on your server
     5. copy data files (reviews & solrcommit) from the link provided below.
     6. Modify config.properties - point to data files correctly on your server, point your webapp directory to your apache home.
     7. pwd to check that you are in SolrNightlyBenchmarks folder. 
     8. mvn clean compile assembly:single
     9. java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar -ProcessLatestCommit true -Housekeeping true 
     
[Download Data Files From This Link](http://212.47.227.9/data/) 
     
## Possible parameters

     * -RunSilently true                       Use this parameter if you do not want any output on console.
     * -ProcessLatestCommit true               Use this parameter if you want the system to look for the latest commit to work on.
     * -ProcessWithCommitID XXXXXXXXXXXX       Use this parameter if you want the system to use the commit hash to work on.
     
     * -ProcessCommitsFromQueue true           Use this parameter if you want the system to work on the commit hash present in the queue.
     * -RegisterLatestCommit true              This parameter is used in conjunction with the last parameter. 
     
     * -Housekeeping true                      Use this parameter to instruct the system to clean up at the end of the work cycle.
     
## Running benchmarks in queue mode

     There is an option to run this framework in queue mode where it tries to capture almost all commits and runs on each. 
     
     The steps to run in this mode is mentioned below.
     
     Configure jenkins to run the following in specified time periods (example every 15 minutes) 
     * java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar -RegisterLatestCommit true
     
     USE this with the following to run the benchmark in queue mode. 
     
     Configure jenkins to run the benchmark utility by running the following (say every midnight)
     * java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar -ProcessCommitsFromQueue true -Housekeeping true 
     
## Where and how to access the output.

     * As soon as the system is up and running, A folder is created as directed by "SolrNightlyBenchmarks.benchmarkAppDirectory" in the properties file.
     * If this folder is mapped to the HTTP server home you can simply open the localhost and you should land on the page where you will have options to view results.
     * If this folder is a local folder, please locate it and click on "index.html" to open the page for viewing the output. 

## Sample output page
     * The page that you will access will look like the following. 

![Alt text](http://www.viveknarang.com/gsoc/snb_screenshot.PNG)

## Notable Additionl features.

     * The framework has the ability to recover from a failed attempt.
         - Example if during execution the benchmark process is killed, the next time when executed the framework will clean up the corrupt files/zombie processes from the last failed run to free the machine from resource wastages.
     * The framework has the ability to notify the end user that a benchmark cycle is running. (A green indicator on top right hand will light up telling the user that currently a fresh benchmark cycle is running.)
     
    
   
     
     

     
      
