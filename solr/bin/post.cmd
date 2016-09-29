@REM
@REM  Licensed to the Apache Software Foundation (ASF) under one or more
@REM  contributor license agreements.  See the NOTICE file distributed with
@REM  this work for additional information regarding copyright ownership.
@REM  The ASF licenses this file to You under the Apache License, Version 2.0
@REM  (the "License"); you may not use this file except in compliance with
@REM  the License.  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off

IF "%OS%"=="Windows_NT" setlocal enabledelayedexpansion enableextensions

REM Determine top-level Solr directory
set SDIR=%~dp0
IF "%SDIR:~-1%"=="\" set SDIR=%SDIR:~0,-1%
set SOLR_TIP=%SDIR%\..
pushd %SOLR_TIP%
set SOLR_TIP=%CD%
popd

REM Used to report errors before exiting the script
set SCRIPT_ERROR=
set NO_USER_PROMPT=0

REM Allow user to import vars from an include file
REM vars set in the include file can be overridden with
REM command line args
IF "%SOLR_INCLUDE%"=="" set "SOLR_INCLUDE=%SOLR_TIP%\bin\solr.in.cmd"
IF EXIST "%SOLR_INCLUDE%" CALL "%SOLR_INCLUDE%"

REM Select HTTP OR HTTPS related configurations
set SOLR_URL_SCHEME=http
set "SOLR_JETTY_CONFIG=--module=http"
set "SOLR_SSL_OPTS= "
IF DEFINED SOLR_SSL_KEY_STORE (
  set "SOLR_JETTY_CONFIG=--module=https"
  set SOLR_URL_SCHEME=https
  set "SCRIPT_ERROR=Solr server directory %SOLR_SERVER_DIR% not found!"
  set "SOLR_SSL_OPTS=-Dsolr.jetty.keystore=%SOLR_SSL_KEY_STORE% -Dsolr.jetty.keystore.password=%SOLR_SSL_KEY_STORE_PASSWORD% -Dsolr.jetty.truststore=%SOLR_SSL_TRUST_STORE% -Dsolr.jetty.truststore.password=%SOLR_SSL_TRUST_STORE_PASSWORD% -Dsolr.jetty.ssl.needClientAuth=%SOLR_SSL_NEED_CLIENT_AUTH% -Dsolr.jetty.ssl.wantClientAuth=%SOLR_SSL_WANT_CLIENT_AUTH%"
  IF DEFINED SOLR_SSL_CLIENT_KEY_STORE  (
    set "SOLR_SSL_OPTS=%SOLR_SSL_OPTS% -Djavax.net.ssl.keyStore=%SOLR_SSL_CLIENT_KEY_STORE% -Djavax.net.ssl.keyStorePassword=%SOLR_SSL_CLIENT_KEY_STORE_PASSWORD% -Djavax.net.ssl.trustStore=%SOLR_SSL_CLIENT_TRUST_STORE% -Djavax.net.ssl.trustStorePassword=%SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD%"
  ) ELSE (
    set "SOLR_SSL_OPTS=%SOLR_SSL_OPTS% -Djavax.net.ssl.keyStore=%SOLR_SSL_KEY_STORE% -Djavax.net.ssl.keyStorePassword=%SOLR_SSL_KEY_STORE_PASSWORD% -Djavax.net.ssl.trustStore=%SOLR_SSL_TRUST_STORE% -Djavax.net.ssl.trustStorePassword=%SOLR_SSL_TRUST_STORE_PASSWORD%"
  )
) ELSE (
  set SOLR_SSL_OPTS=
)

REM Set the SOLR_TOOL_HOST variable for use when connecting to a running Solr instance
IF NOT "%SOLR_HOST%"=="" (
  set "SOLR_TOOL_HOST=%SOLR_HOST%"
) ELSE (
  set "SOLR_TOOL_HOST=localhost"
)

REM Verify Java is available
IF DEFINED SOLR_JAVA_HOME set "JAVA_HOME=%SOLR_JAVA_HOME%"
REM Try to detect JAVA_HOME from the registry
IF NOT DEFINED JAVA_HOME (
  FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment" /v CurrentVersion') DO set CurVer=%%B
  FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment\!CurVer!" /v JavaHome') DO (
    set "JAVA_HOME=%%B"
  )
)
IF NOT DEFINED JAVA_HOME goto need_java_home
set JAVA_HOME=%JAVA_HOME:"=%
IF %JAVA_HOME:~-1%==\ SET JAVA_HOME=%JAVA_HOME:~0,-1%
IF NOT EXIST "%JAVA_HOME%\bin\java.exe" (
  set "SCRIPT_ERROR=java.exe not found in %JAVA_HOME%\bin. Please set JAVA_HOME to a valid JRE / JDK directory."
  goto err
)
set "JAVA=%JAVA_HOME%\bin\java"
CALL :resolve_java_info
IF !JAVA_MAJOR_VERSION! LSS 8 (
  set "SCRIPT_ERROR=Java 1.8 or later is required to run Solr. Current Java version is: !JAVA_VERSION_INFO!"
  goto err
)

REM RUN the Post tool
"%JAVA%" -Dlog4j.configuration=file:%SOLR_TIP%/server/resources/log4j.properties ^
  -cp "%SOLR_TIP%\server\solr-webapp\webapp\WEB-INF\lib\*;%SOLR_TIP%\server\lib\ext\*" ^
  org.apache.solr.util.PostCLI %*


:need_java_home
@echo Please set the JAVA_HOME environment variable to the path where you installed Java 1.8+
goto done

:need_java_vers
@echo Java 1.8 or later is required to run Solr.
goto done

:err
@echo.
@echo ERROR: !SCRIPT_ERROR!
@echo.
exit /b 1

:done
ENDLOCAL
exit /b 0

REM Tests what Java we have and sets some global variables
:resolve_java_info

CALL :resolve_java_vendor

set JAVA_MAJOR_VERSION=0
set JAVA_VERSION_INFO=
set JAVA_BUILD=0

"%JAVA%" -version 2>&1 | findstr /i "version" > javavers
set /p JAVAVEROUT=<javavers
del javavers

for /f "tokens=3" %%a in ("!JAVAVEROUT!") do (
  set JAVA_VERSION_INFO=%%a
  REM Remove surrounding quotes
  set JAVA_VERSION_INFO=!JAVA_VERSION_INFO:"=!

  REM Extract the major Java version, e.g. 7, 8, 9, 10 ...
  for /f "tokens=2 delims=." %%a in ("!JAVA_VERSION_INFO!") do (
    set JAVA_MAJOR_VERSION=%%a
  )

  REM Don't look for "_{build}" if we're on IBM J9.
  if NOT "%JAVA_VENDOR%" == "IBM J9" (
    for /f "delims=_ tokens=2" %%a in ("!JAVA_VERSION_INFO!") do (
      set /a JAVA_BUILD=%%a
    )
  )
)
GOTO :eof

REM Set which JVM vendor we have
:resolve_java_vendor
set "JAVA_VENDOR=Oracle"
"%JAVA%" -version 2>&1 | findstr /i "IBM J9" > javares
set /p JAVA_VENDOR_OUT=<javares
del javares
if NOT "%JAVA_VENDOR_OUT%" == "" (
  set "JAVA_VENDOR=IBM J9"
)

set JAVA_VENDOR_OUT=
GOTO :eof

REM Safe echo which does not mess with () in strings
:safe_echo
set "eout=%1"
set eout=%eout:"=%
echo !eout!
GOTO :eof