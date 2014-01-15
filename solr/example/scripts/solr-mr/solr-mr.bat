
set JVM=java

REM  Find location of this script

set SDIR=%~dp0
if "%SDIR:~-1%"=="\" set SDIR=%SDIR:~0,-1%

"%JVM%" -classpath "%SDIR%\..\..\..\dist\*:%SDIR%\..\..\..\contrib\solr-mr\lib\*:%SDIR%\..\..\..\contrib\solr-morphlines-core\lib\*:%SDIR%\..\..\..\contrib\solr-morphlines-cell\lib\*:%SDIR%\..\..\..\contrib\extraction\lib\*:%SDIR%\..\..\solr-webapp\solr\WEB-INF\lib\*:%SDIR%\..\..\lib\ext\*" org.apache.solr.hadoop.MapReduceIndexerTool %*
