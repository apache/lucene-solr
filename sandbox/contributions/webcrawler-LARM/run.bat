rmdir /s /q -r logs
mkdir logs
java -server -Xmx400mb -classpath classes;libs/jakarta-oro-2.0.5.jar de.lanlab.larm.fetcher.FetcherMain -start http://www.cis.uni-muenchen.de/ -restrictto http://.*\.uni-muenchen\.de.* -threads 15  
