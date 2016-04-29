@echo off
REM You can override pass the following parameters to this script:
REM 

set JVM=java

REM  Find location of this script

set SDIR=%~dp0
if "%SDIR:~-1%"=="\" set SDIR=%SDIR:~0,-1%

if defined LOG4J_PROPS (
  set "LOG4J_CONFIG=file:%LOG4J_PROPS%"
) else (
  set "LOG4J_CONFIG=file:%SDIR%\log4j.properties"
)

"%JVM%" -Dlog4j.configuration="%LOG4J_PROPS%" -classpath "%SDIR%\..\..\solr-webapp\webapp\WEB-INF\lib\*;%SDIR%\..\..\lib\ext\*" org.apache.solr.cloud.ZkCLI %*
