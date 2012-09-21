REM You can override pass the following parameters to this script:
REM 

set JVM=java

REM  Find location of this script

set SDIR=%~dp0
if "%SDIR:~-1%"=="\" set SDIR=%SDIR:~0,-1%

     
"%JVM%" -classpath "%SDIR%\..\solr-webapp\webapp\WEB-INF\lib\*" org.apache.solr.cloud.ZkCLI %*
