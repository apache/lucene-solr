# SolrNightlyBenchmarks  [![Travis branch](https://img.shields.io/travis/rust-lang/rust/master.svg)]()

A comprehensive Solr performance benchmark framework.

## [A] Server/OS Requirements

      * Java Version: 1.8.0_131 or above
      * Linux OS
      * Install Apache Ivy
      * Install lsof utility
      * Ant Version 1.9.2 or above
      * Apache Maven 3.3.9 or above
      * git version 2.11.1 or above
      * lshw utility installed on your linux machine
      * RAM 16 GB and above
      * CPU as strong as possible
      
## [B] Data Files

     * These are the files that are used by this platform for benchmarking purposes. 
     * Please access the /scripts folder for the shell utility that is helpful in downloading data files. 
     * Please run the shell script 'download.sh' to download the required files.
     * Please ensure that these files are downloaded properly and completely.  

## [C] Steps to launch

     1. git clone https://github.com/viveknarang/lucene-solr.git
     2. git checkout 'SolrNightlyBenchmarks'
     3. cd /dev-tools/
     4. cp -r solrnightlybenchmarks to target location on your server
     5  VERY IMPORTANT: Modify config.properties - point to data files correctly on your server.
     6  pwd to check that you are in solrnightlybenchmarks folder
     7  mvn clean compile assembly:single
     8  mvn jetty:run 
     9  java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
        --latest-commit --clean-up 
     10 mvn jetty:stop
     
     Note: Please use separate terminals for steps 8, 9 and 10. Also please make sure to 
     execute step 8 before step 9 and also wait until Jetty server is started. 
     Step 10 is optional and only needed when you want to shutdown the server.
     

## [C-1] Possible parameters

     Below is a list of valid parameters usable on step 8 in the section above. 

     * --silent                                Use this parameter if you do not want any output on console.
     
     * --archive                               Use this parameter to archive the data in the location as 
                                                  defined in properties file.
     * --clear-data                            Use this parameter to clear the data folder in the webapp directory.
     
     * --latest-commit                         Use this parameter if you want the system to look for the 
                                                  latest commit to work on.
     * --commit-id XXXXXXXXXXXX                Use this parameter if you want the system to use the commit 
                                                  hash to work on.
     * --clean-up                              Use this parameter to instruct the system to clean up at the 
                                                  end of the work cycle.
     * --test-with-number-of-documents XXXX    Use this parameter to specify the subset of the available documents 
                                                  to test with. Valid value lies in 
                                                  (with en-wiki-data-2G-modified.csv data file) between 1 and 347776.
     
## [D] Accessing Output

     * localhost:4444 
  
## [D-1] Sample Output
     * The page that you will access will look like the following. 

![Alt text](http://www.viveknarang.com/gsoc/snb_screenshot5.PNG)
