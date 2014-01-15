
set JVM=java

REM  Find location of this script

set SDIR=%~dp0
if "%SDIR:~-1%"=="\" set SDIR=%SDIR:~0,-1%

"%JVM%" -classpath "%SDIR%\..\..\..\dist\*:%SDIR%\..\..\..\contrib\map-reduce\lib\*:%SDIR%\..\..\..\contrib\morphlines-core\lib\*:%SDIR%\..\..\..\contrib\morphlines-cell\lib\*:%SDIR%\..\..\..\contrib\extraction\lib\*:%SDIR%\..\..\solr-webapp\webapp\WEB-INF\lib\*:%SDIR%\..\..\lib\ext\*" org.apache.solr.hadoop.MapReduceIndexerTool %*
