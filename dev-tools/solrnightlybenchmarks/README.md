# Solr Nightly Benchmarks  [![Travis branch](https://img.shields.io/travis/rust-lang/rust/master.svg)]()

A comprehensive Solr performance benchmark framework.

Please read the following sections in order and follow instructions. 

## [A] Server/OS Requirements

      * Java Version: 1.8.0_131 or above
      * GNU/Linux
      * lsof, lshw utilities
      * Apache Ivy 2.4.0 or above
      * Ant Version 1.9.2 or above
      * Apache Maven 3.3.9 or above
      * git version 2.11.1 or above
      * RAM 16 GB and above
      * At least a quad-core CPU
      * At least 10GB of free disk space
      
## [B] Steps to launch

     Note: Please checkout in a location with ample free disk space (at least 10GB)

     1. git clone https://github.com/viveknarang/lucene-solr.git --branch SolrNightlyBenchmarks solr-nightly-benchmarks
     2. cd solr-nightly-benchmarks/dev-tools/solrnightlybenchmarks
     3. cd data; ../scripts/download.sh; cd ..
     4. nohup mvn jetty:run & > logs/jetty.log
     5. mvn clean compile assembly:single
     6. java -jar target/org.apache.solr.tests.nightlybenchmarks-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
             --latest-commit --clean-up 
     7. mvn jetty:stop     # To stop the webserver that hosts the results/reports
     

## [C] Parameters

     Below is a list of valid parameters usable on step 6 in the section above. 
  
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
     * --use-sample-dataset X.XX               Use this option when you want to work in dev-mode (i.e while enhancing
                                               /debugging this project.). Please also pass a value in the range 0.01 to 1
                                               with this parameter. This value is the percentage of data set that is used
                                               in this mode. 
     
## [D] Accessing Output

     * Please open localhost:4444 OR IP:4444 on your favorite browser.
     * If you plan to change jetty port number please use your configured port number instead of 4444.
  
## [D-1] Sample Output
     * The page that you will access will look like the following. 

![Alt text](http://www.viveknarang.com/gsoc/snb_screenshot5.PNG)
