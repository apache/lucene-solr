#!/bin/sh
rm -r logs
mkdir logs
java -server -Xmx400mb -classpath classes:lib/jakarta-oro-2.0.5.jar de.lanlab.larm.fetcher.FetcherMain -start http://www.cis.uni-muenchen.de/ -restrictto http://[^/]*\.uni-muenchen\.de.* -threads 15  
