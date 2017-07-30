# SolrNightlyBenchmarks  [![Travis branch](https://img.shields.io/travis/rust-lang/rust/master.svg)]()

A comprehensive Solr performance benchmark framework.

## Server/OS Requirements

      * Java Version: 1.8.0_131 and above
      * Linux OS
      * Install Apache Ivy
      * Install lsof utility
      * Ant Version 1.9.2 or above
      * Apache Maven 3.0.5 and above
      * git version 2.11.1 and above
      * lshw utility installed on your linux machine
      * [OPTIONAL] Apache HTTP webserver 
        (If you want this framwork's output to be accessible over a network through a browser)
      * RAM 16 GB and above
      * CPU as strong as possible

## Steps to launch

     1. git clone https://github.com/viveknarang/lucene-solr.git
     2. git checkout 'SolrNightlyBenchmarks'
     3. cd /dev-tools/
     4. cp -r SolrNightlyBenchmarks to target location on your server
     5  VERY IMPORTANT: Modify config.properties - point to data files correctly on your server, 
        point your webapp directory to your apache HTTP server home
     6  pwd to check that you are in SolrNightlyBenchmarks folder
     7  mvn clean compile assembly:single
     8  java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
        --latest-commit --clean-up 

## Possible parameters

     Below is a list of valid parameters usable on step 8 in the section above. On the left of each line below, 
     the entry starting with -- is the valid parameter and to the right of it is its short descritpion.  

     * --silent                                Use this parameter if you do not want any output on console.
     
     * --archive                               Use this parameter to archive the data in the location as 
                                                  defined in properties file.
     * --clear-data                            Use this parameter to clear the data folder in the webapp directory.
     
     * --latest-commit                         Use this parameter if you want the system to look for the 
                                                  latest commit to work on.
     * --commit-id XXXXXXXXXXXX                Use this parameter if you want the system to use the commit 
                                                  hash to work on.
     * --from-queue                            Use this parameter if you want the system to work on the 
                                                  commit hash present in the queue.
     * --register-commit                       This parameter is used in conjunction with the last parameter. 
     
     * --generate-data-file                    Generates a fresh test data file with 1 million records 
                                                  ~3.7GB size, in the webapp/data directory.     
     * --clean-up                              Use this parameter to instruct the system to clean up at the 
                                                  end of the work cycle.
     * --test-with-number-of-documents XXXX    Use this parameter to specify the subset of the available documents 
                                                  to test with. Valid value lies in 
                                                  (with en-wiki-data-2G-modified.csv data file) between 1 and 347776.
     
## Accessing Output

     * As soon as the system is up and running, a folder is created as directed by 
         SolrNightlyBenchmarks.benchmarkAppDirectory parameter in the properties file. An app 
         (can be used both, offline and online through HTTP server) is deployed in the folder 
         automatically. 
     * If this folder is mapped to the HTTP server home you can simply access the app using 
         the IP address and port of the webserver and you should land on the page where you 
         will have options to view benchmark results.
     * If this folder is a local folder and not the home folder for a web server, 
         please locate it and open "index.html" using your favorite browser. 

## Sample Output
     * The page that you will access will look like the following. 

![Alt text](http://www.viveknarang.com/gsoc/snb_screenshot5.PNG)

## Dependent Data Files 

     * The files referred here are the files that have the data (example: file containing modified 
          wikipedia data used during indexing throughput tests.
     * The system has the ability to check and download the required data files from the source, 
          as specified in the properties file.
     * For any reason, If you want to manually download/view files please use the link provided below. 
          Please note that this step is not required when the platform is configured properly to 
          download required files.
     
[Download Data Files From This Link](http://212.47.227.9/data/)     
