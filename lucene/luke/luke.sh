#!/bin/bash

if [[ -d `echo $LUKE_PATH` ]]; then
  java -jar $LUKE_PATH/target/luke-javafx-with-deps.jar
else
  echo "Unable to find the LUKE_PATH environnement variable..."
  echo "Assuming you're running from the root folder of luke..."
  nohup java -jar target/luke-javafx-with-deps.jar > /dev/null 2>&1 &
fi
#
# In order to start luke with custom components (a custom analyzer class extending org.apache.lucene.analysis.Analyzer or a custom Codec) run:
# java -cp target/luke-with-deps.jar:/path/to/jar-with-custom-component.jar org.getopt.luke.Luke
# your analyzer should appear in the drop-down menu with analyzers on the Search tab and your Codec should be automatically registered
# java -cp target/luke-with-deps.jar:/home/dmitry/projects/github/suggestinganalyzer/target/suggestinganalyzer-1.0-SNAPSHOT.jar org.getopt.luke.Luke
