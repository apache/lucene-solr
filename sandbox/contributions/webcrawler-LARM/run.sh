#!/bin/sh
rm -r logs
mkdir logs
java -server -Xmx400mb -classpath classes:libs/jakarta-oro-2.0.5.jar de.lanlab.larm.fetcher.FetcherMain -start http://your.server.here/ -restrictto http://[^/]*\.your\.server\.here.* -threads 15  
