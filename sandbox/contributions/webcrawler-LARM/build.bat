@echo off

rem clean
echo cleaning
rmdir /s /q  build
rmdir /s /q classes
rmdir /s /q cachingqueue
rmdir /s /q logs

rem build
echo making build directory
mkdir build
cd build
echo extracting http client
jar xvf ../libs/HTTPClient.zip >nul
cd ..
xcopy /s src\*.java build
mkdir classes
echo compiling
javac -g -d classes -sourcepath build build/HTTPClient/*.java
javac -g -classpath ./libs/jakarta-oro-2.0.5.jar -d classes -sourcepath build build/de/lanlab/larm/fetcher/FetcherMain.java
