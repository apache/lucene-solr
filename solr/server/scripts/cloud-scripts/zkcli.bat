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

REM Settings for ZK ACL
REM set SOLR_ZK_CREDS_AND_ACLS=-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider ^
REM  -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider ^
REM  -DzkDigestUsername=admin-user -DzkDigestPassword=CHANGEME-ADMIN-PASSWORD ^
REM  -DzkDigestReadonlyUsername=readonly-user -DzkDigestReadonlyPassword=CHANGEME-READONLY-PASSWORD

"%JVM%" %SOLR_ZK_CREDS_AND_ACLS% %ZKCLI_JVM_FLAGS% -Dlog4j.configuration="%LOG4J_CONFIG%" ^
-classpath "%SDIR%\..\..\solr-webapp\webapp\WEB-INF\lib\*;%SDIR%\..\..\lib\ext\*;%SDIR%\..\..\lib\*" org.apache.solr.cloud.ZkCLI %*
