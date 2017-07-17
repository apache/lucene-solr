# SolrNightlyBenchmarks  [![Travis branch](https://img.shields.io/travis/rust-lang/rust/master.svg)]()

A comprehensive Solr performance benchmark framework.

## Requirements

      * Java Version: 1.8.0_131 and above
      * Linux OS
      * Apache Maven 3.0.5 and above
      * git version 2.11.1 and above
      * lshw utility installed on your linux machine
      * [OPTIONAL] Apache HTTP webserver (If you want this framwork's output to be accessible over a network through a browser)
      * RAM 16 GB and above
      * CPU as strong as possible

## Steps to run

     1. git clone https://github.com/viveknarang/lucene-solr.git
     2. git checkout 'SolrNightlyBenchmarks'
     3. cd /dev-tools/
     4. cp -r SolrNightlyBenchmarks to target location on your server
     5. Copy data files (test-data-file-1M.csv, Numeric-Term-Query.txt, Numeric-Pair-Query-Data.txt & Sorted-Numeric-Pair-Query-Data.txt) from the link provided below
     6. VERY IMPORTANT: Modify config.properties - point to data files correctly on your server, point your webapp directory to your apache HTTP server home
     7. pwd to check that you are in SolrNightlyBenchmarks folder
     8. mvn clean compile assembly:single
     9. java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar --latest-commit --clean-up 
     
[Download Data Files From This Link](http://212.47.227.9/data/) 

## Data structure and data file details

     The data currently being used has the following data structure.     
               * { Int(id), String(Text), Int(Numeric), Long(Numeric), Alphanumeric(Catgory), String(Text) }
     The record size averages around ~3.8KB
     The data file name is "test-data-file-1M.csv" and the file is around 3.8GB in size.

## Possible parameters

     * --silent                                Use this parameter if you do not want any output on console.
     * --latest-commit                         Use this parameter if you want the system to look for the latest commit to work on.
     * --commit-id XXXXXXXXXXXX                Use this parameter if you want the system to use the commit hash to work on.
     * --from-queue                            Use this parameter if you want the system to work on the commit hash present in the queue.
     * --register-commit                       This parameter is used in conjunction with the last parameter. 
     * --generate-data-file                    Generates a fresh test data file with 1 million records ~3.7GB size, in the webapp/data directory.     
     * --clean-up                              Use this parameter to instruct the system to clean up at the end of the work cycle.
     * --test-with-number-of-documents XXXX    Use this parameter to specify the subset of the available documents to test with. Valid value lies in (with test-data-file-1M.csv data file) between 1 and 1000000.
     
## Running benchmarks in queue mode

     There is an option to run this framework in queue mode where it tries to capture almost all commits and runs on each. 
     
     The steps to run in this mode is mentioned below.
     
     Configure jenkins to run the following in specified time periods (example every 15 minutes) 
     * java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar -RegisterLatestCommit
     
     USE this with the following to run the benchmark in queue mode. 
     
     Configure jenkins to run the benchmark utility by running the following (say every midnight)
     * java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar -ProcessCommitsFromQueue -Housekeeping  
     
## Where and how to access the output.

     * As soon as the system is up and running, A folder is created as directed by "SolrNightlyBenchmarks.benchmarkAppDirectory" in the properties file. An app (can be used both, offline and online through HTTP server) is deployed in the folder automatically. 
     * If this folder is mapped to the HTTP server home you can simply open the localhost and you should land on the page where you will have options to view results.
     * If this folder is a local folder, please locate it and open "index.html" with your favorite browser to open the page for viewing the output. 

## Sample output page
     * The page that you will access will look like the following. 

![Alt text](http://www.viveknarang.com/gsoc/snb_screenshot.PNG)

## Notable Additional features

     * The framework has the ability to recover from a failed attempt.
         - Example if during execution the benchmark process is killed, the next time when executed the framework will clean up the corrupt files/zombie processes from the last failed run to free the machine from resource wastages.
     * The framework has the ability to notify the end user that a benchmark cycle is running. (A green indicator on top right hand will light up telling the user that currently a fresh benchmark cycle is running.) Once the cycle is complete a refresh button will appear on top righ hand corner. 
     * The webapp is deployed automatically when not present.  
     
## Known Limitations
     * The current design of this framework does not allow two or more instances of this system running on a single machine TOGETHER. For now, please do not run two instances of this system running on the same machine at the same time (one might kill the other). 
     
## Important Note
     * In the event of a insufficient resources (CPU and RAM), due to hosting this system on a weak machine, the metrics reported will fluctuate and will NOT be reliable. Please consider hosting this system on a strong machine (free from noise by other processes running on it). 
    
    
   
     
     

     
      
