#!/bin/sh

#clean
echo cleaning
rm -r build
rm -r classes
rm -r cachingqueue
rm -r logs

#build
echo making build directory
mkdir build
cd build
echo extracting http client
jar xvf ../lib/HTTPClient.zip >/dev/nul
cd ..
cp -r src/* build
mkdir classes
echo compiling
javac -g -d classes -sourcepath build build/HTTPClient/*.java
javac -g -classpath ./lib/jakarta-oro-2.0.5.jar -d classes -sourcepath build build/de/lanlab/larm/fetcher/FetcherMain.java


