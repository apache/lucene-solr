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
IF "%SOLR_INCLUDE%"=="" set SOLR_INCLUDE=solr.in.cmd
IF EXIST "%SOLR_INCLUDE%" CALL "%SOLR_INCLUDE%"

REM Verify Java is available
IF DEFINED SOLR_JAVA_HOME set "JAVA_HOME=%SOLR_JAVA_HOME%"
IF NOT DEFINED JAVA_HOME goto need_java_home
set JAVA_HOME=%JAVA_HOME:"=%
"%JAVA_HOME%"\bin\java -version:1.8 -version > nul 2>&1
IF ERRORLEVEL 1 "%JAVA_HOME%"\bin\java -version:1.7 -version > nul 2>&1
IF ERRORLEVEL 1 goto need_java_vers
set "JAVA=%JAVA_HOME%\bin\java"

REM See SOLR-3619
IF EXIST "%SOLR_TIP%\server\start.jar" (
  set "DEFAULT_SERVER_DIR=%SOLR_TIP%\server"
) ELSE (
  set "DEFAULT_SERVER_DIR=%SOLR_TIP%\example"
)

set FIRST_ARG=%1

IF [%1]==[] goto usage

IF "%1"=="-help" goto usage
IF "%1"=="-usage" goto usage
IF "%1"=="/?" goto usage
IF "%1"=="-i" goto get_info
IF "%1"=="-info" goto get_info

REM Only allow the command to be the first argument, assume start if not supplied
IF "%1"=="start" goto set_script_cmd
IF "%1"=="stop" goto set_script_cmd
IF "%1"=="restart" goto set_script_cmd
IF "%1"=="healthcheck" (
  REM healthcheck uses different arg parsing strategy
  set SCRIPT_CMD=healthcheck
  SHIFT
  goto parse_healthcheck_args
)
IF "%1"=="create_core" (
  set SCRIPT_CMD=create_core
  SHIFT
  goto parse_create_args
)
IF "%1"=="create_collection" (
  set SCRIPT_CMD=create_collection
  SHIFT
  goto parse_create_args
)
goto parse_args

:usage
IF NOT "%SCRIPT_ERROR%"=="" ECHO %SCRIPT_ERROR%
IF [%FIRST_ARG%]==[] goto script_usage
IF "%FIRST_ARG%"=="-help" goto script_usage
IF "%FIRST_ARG%"=="-usage" goto script_usage
IF "%FIRST_ARG%"=="/?" goto script_usage
IF "%SCRIPT_CMD%"=="start" goto start_usage
IF "%SCRIPT_CMD%"=="restart" goto start_usage
IF "%SCRIPT_CMD%"=="stop" goto stop_usage
IF "%SCRIPT_CMD%"=="healthcheck" goto healthcheck_usage
IF "%SCRIPT_CMD%"=="create_core" goto create_core_usage
IF "%SCRIPT_CMD%"=="create_collection" goto create_collection_usage
goto done

:script_usage
@echo.
@echo Usage: solr COMMAND OPTIONS
@echo        where COMMAND is one of: start, stop, restart, healthcheck, create_core, create_collection
@echo.
@echo   Standalone server example (start Solr running in the background on port 8984):
@echo.
@echo     solr start -p 8984
@echo.
@echo   SolrCloud example (start Solr running in SolrCloud mode using localhost:2181 to connect to ZooKeeper, with 1g max heap size and remote Java debug options enabled):
@echo.
@echo     solr start -c -m 1g -z localhost:2181 -a "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044"
@echo.
@echo Pass -help after any COMMAND to see command-specific usage information,
@echo   such as:    solr start -help or solr stop -help
@echo.
goto done

:start_usage
@echo.
@echo Usage: solr %SCRIPT_CMD% [-f] [-c] [-h hostname] [-p port] [-d directory] [-z zkHost] [-m memory] [-e example] [-s solr.solr.home] [-a "additional-options"] [-V]
@echo.
@echo   -f            Start Solr in foreground; default starts Solr in the background
@echo                   and sends stdout / stderr to solr-PORT-console.log
@echo.
@echo   -c or -cloud  Start Solr in SolrCloud mode; if -z not supplied, an embedded ZooKeeper
@echo                   instance is started on Solr port+1000, such as 9983 if Solr is bound to 8983
@echo.
@echo   -h host       Specify the hostname for this Solr instance
@echo.
@echo   -p port       Specify the port to start the Solr HTTP listener on; default is 8983
@echo.
@echo   -d dir        Specify the Solr server directory; defaults to example
@echo.
@echo   -z zkHost     ZooKeeper connection string; only used when running in SolrCloud mode using -c
@echo                   To launch an embedded ZooKeeper instance, don't pass this parameter.
@echo.
@echo   -m memory     Sets the min (-Xms) and max (-Xmx) heap size for the JVM, such as: -m 4g
@echo                   results in: -Xms4g -Xmx4g; by default, this script sets the heap size to 512m
@echo.
@echo   -s dir        Sets the solr.solr.home system property; Solr will create core directories under
@echo                   this directory. This allows you to run multiple Solr instances on the same host
@echo                   while reusing the same server directory set using the -d parameter. If set, the
@echo                   specified directory should contain a solr.xml file. The default value is example/solr.
@echo                   This parameter is ignored when running examples (-e), as the solr.solr.home depends
@echo                   on which example is run.
@echo.
@echo   -e example    Name of the example to run; available examples:
@echo       cloud:          SolrCloud example
@echo       techproducts:   Comprehensive example illustrating many of Solr's core capabilities
@echo       dih:            Data Import Handler
@echo       schemaless:     Schema-less example
@echo.
@echo   -a opts       Additional parameters to pass to the JVM when starting Solr, such as to setup
@echo                 Java debug options. For example, to enable a Java debugger to attach to the Solr JVM
@echo                 you could pass: -a "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=18983"
@echo                 In most cases, you should wrap the additional parameters in double quotes.
@echo.
@echo   -noprompt     Don't prompt for input; accept all defaults when running examples that accept user input
@echo.
@echo   -V            Verbose messages from this script
@echo.
goto done

:stop_usage
@echo.
@echo Usage: solr stop [-k key] [-p port]
@echo.
@echo  -k key      Stop key; default is solrrocks
@echo.
@echo  -p port     Specify the port to start the Solr HTTP listener on; default is 8983
@echo.
@echo  -all        Find and stop all running Solr servers on this host
@echo.
@echo  -V          Verbose messages from this script
@echo.
goto done

:healthcheck_usage
@echo.
@echo Usage: solr healthcheck [-c collection] [-z zkHost]
@echo.
@echo   -c collection  Collection to run healthcheck against.
@echo.
@echo   -z zkHost      ZooKeeper connection string; default is localhost:9983
@echo.
goto done

:create_core_usage
echo.
echo Usage: solr create_core [-n name] [-c configset]
echo.
echo   -n name       Name of core to create
echo.
echo   -c configset  Name of configuration directory to use, valid options are:
echo       basic_configs: Minimal Solr configuration
echo       data_driven_schema_configs: Managed schema with field-guessing support enabled
echo       sample_techproducts_configs: Example configuration with many optional features enabled to
echo          demonstrate the full power of Solr
echo       If not specified, default is: data_driven_schema_configs
echo.
goto done

:create_collection_usage
echo.
echo Usage: solr create_collection [-n name] [-c configset] [-shards #] [-replicationFactor #]
echo.
echo   -n name               Name of collection to create
echo.
echo   -c configset          Name of configuration directory to use, valid options are:
echo       basic_configs: Minimal Solr configuration
echo       data_driven_schema_configs: Managed schema with field-guessing support enabled
echo       sample_techproducts_configs: Example configuration with many optional features enabled to
echo          demonstrate the full power of Solr
echo       If not specified, default is: data_driven_schema_configs
echo.
echo   -shards #             Number of shards to split the collection into
echo.
echo   -replicationFactor #  Number of copies of each document in the collection
echo.
goto done

REM Really basic command-line arg parsing
:parse_args
IF "%SCRIPT_CMD%"=="" set SCRIPT_CMD=start
IF [%1]==[] goto process_script_cmd
IF "%1"=="-help" goto usage
IF "%1"=="-usage" goto usage
IF "%1"=="/?" goto usage
IF "%1"=="-f" goto set_foreground_mode
IF "%1"=="-foreground" goto set_foreground_mode
IF "%1"=="-V" goto set_verbose
IF "%1"=="-verbose" goto set_verbose
IF "%1"=="-c" goto set_cloud_mode
IF "%1"=="-cloud" goto set_cloud_mode
IF "%1"=="-d" goto set_server_dir
IF "%1"=="-dir" goto set_server_dir
IF "%1"=="-s" goto set_solr_home_dir
IF "%1"=="-solr.home" goto set_solr_home_dir
IF "%1"=="-e" goto set_example
IF "%1"=="-example" goto set_example
IF "%1"=="-h" goto set_host
IF "%1"=="-host" goto set_host
IF "%1"=="-m" goto set_memory
IF "%1"=="-memory" goto set_memory
IF "%1"=="-p" goto set_port
IF "%1"=="-port" goto set_port
IF "%1"=="-z" goto set_zookeeper
IF "%1"=="-zkhost" goto set_zookeeper
IF "%1"=="-a" goto set_addl_opts
IF "%1"=="-addlopts" goto set_addl_opts
IF "%1"=="-noprompt" goto set_noprompt
IF "%1"=="-k" goto set_stop_key
IF "%1"=="-key" goto set_stop_key
IF "%1"=="-all" goto set_stop_all
IF NOT "%1"=="" goto invalid_cmd_line

:set_script_cmd
set SCRIPT_CMD=%1
SHIFT
goto parse_args

:set_foreground_mode
set FG=1
SHIFT
goto parse_args

:set_verbose
set verbose=1
SHIFT
goto parse_args

:set_cloud_mode
set SOLR_MODE=solrcloud
SHIFT
goto parse_args

:set_server_dir

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Directory name is required!
  goto invalid_cmd_line
)
set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected directory but found %2 instead!
  goto invalid_cmd_line
)

REM See if they are using a short-hand name relative from the Solr tip directory
IF EXIST "%SOLR_TIP%\%~2" (
  set "SOLR_SERVER_DIR=%SOLR_TIP%\%~2"
) ELSE (
  set "SOLR_SERVER_DIR=%~2"
)
SHIFT
SHIFT
goto parse_args

:set_solr_home_dir

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Directory name is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected directory but found %2 instead!
  goto invalid_cmd_line
)
set "SOLR_HOME=%~2"
SHIFT
SHIFT
goto parse_args

:set_example

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Example name is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected example name but found %2 instead!
  goto invalid_cmd_line
)

set EXAMPLE=%~2
SHIFT
SHIFT
goto parse_args

:set_memory

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Memory setting is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected memory setting but found %2 instead!
  goto invalid_cmd_line
)

set SOLR_HEAP=%~2
SHIFT
SHIFT
goto parse_args

:set_host
set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Hostname is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected hostname but found %2 instead!
  goto invalid_cmd_line
)

set SOLR_HOST=%~2
SHIFT
SHIFT
goto parse_args

:set_port
set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Port is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected port but found %2 instead!
  goto invalid_cmd_line
)

set SOLR_PORT=%~2
SHIFT
SHIFT
goto parse_args

:set_stop_key
set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Stop key is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected stop key but found %2 instead!
  goto invalid_cmd_line
)
set STOP_KEY=%~2
SHIFT
SHIFT
goto parse_args

:set_stop_all
set STOP_ALL=1
SHIFT
goto parse_args

:set_zookeeper

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=ZooKeeper connection string is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected ZooKeeper connection string but found %2 instead!
  goto invalid_cmd_line
)

set "ZK_HOST=%~2"
SHIFT
SHIFT
goto parse_args

:set_addl_opts
set "arg=%~2"
set "SOLR_ADDL_ARGS=%~2"
SHIFT
SHIFT
goto parse_args

:set_noprompt
set NO_USER_PROMPT=1
SHIFT
goto parse_args

REM Perform the requested command after processing args
:process_script_cmd

IF "%verbose%"=="1" (
  @echo Using Solr root directory: %SOLR_TIP%
  @echo Using Java: %JAVA%
  "%JAVA%" -version
)

IF NOT "%SOLR_HOST%"=="" (
  set SOLR_HOST_ARG=-Dhost=%SOLR_HOST%
) ELSE (
  set SOLR_HOST_ARG=
)

REM TODO: Change this to "server" when we resolve SOLR-3619
IF "%SOLR_SERVER_DIR%"=="" set SOLR_SERVER_DIR=%DEFAULT_SERVER_DIR%

IF NOT EXIST "%SOLR_SERVER_DIR%" (
  set SCRIPT_ERROR=Solr server directory %SOLR_SERVER_DIR% not found!
  goto err
)

IF "%EXAMPLE%"=="" (
  REM SOLR_HOME just becomes serverDir/solr
) ELSE IF "%EXAMPLE%"=="techproducts" (
  set "SOLR_HOME=%SOLR_TIP%\server\solr"
) ELSE IF "%EXAMPLE%"=="cloud" (
  set SOLR_MODE=solrcloud
  goto cloud_example_start
) ELSE IF "%EXAMPLE%"=="dih" (
  set "SOLR_HOME=%SOLR_TIP%\example\example-DIH\solr"
) ELSE IF "%EXAMPLE%"=="schemaless" (
  set "SOLR_HOME=%SOLR_TIP%\server\solr"
) ELSE (
  @echo.
  @echo 'Unrecognized example %EXAMPLE%!'
  @echo.
  goto start_usage
)

:start_solr
IF "%SOLR_HOME%"=="" set "SOLR_HOME=%SOLR_SERVER_DIR%\solr"
IF NOT EXIST "%SOLR_HOME%\" (
  IF EXIST "%SOLR_SERVER_DIR%\%SOLR_HOME%" (
    set "SOLR_HOME=%SOLR_SERVER_DIR%\%SOLR_HOME%"
  ) ELSE (
    set SCRIPT_ERROR=Solr home directory %SOLR_HOME% not found!
    goto err
  )
)

IF NOT EXIST "%SOLR_HOME%\solr.xml" (
  set SCRIPT_ERROR=Solr home directory %SOLR_HOME% must contain solr.xml!
  goto err
)

IF "%STOP_KEY%"=="" set STOP_KEY=solrrocks

@REM stop logic here
IF "%SCRIPT_CMD%"=="stop" (
  IF "%SOLR_PORT%"=="" (
    IF "%STOP_ALL%"=="1" (
      for /f "usebackq" %%i in (`dir /b %SOLR_TIP\bin% ^| findstr /i "^solr-.*\.port$"`) do (
        set SOME_SOLR_PORT=
        For /F "Delims=" %%J In (%SOLR_TIP%\bin\%%i) do set SOME_SOLR_PORT=%%~J
        if NOT "!SOME_SOLR_PORT!"=="" (
          for /f "tokens=2,5" %%j in ('netstat -aon ^| find /i "listening" ^| find ":!SOME_SOLR_PORT!"') do (
            @echo Stopping Solr running on port !SOME_SOLR_PORT!
            set /A STOP_PORT=!SOME_SOLR_PORT! - 1000
            "%JAVA%" -jar "%SOLR_SERVER_DIR%\start.jar" STOP.PORT=!STOP_PORT! STOP.KEY=%STOP_KEY% --stop
            del %SOLR_TIP%\bin\solr-!SOME_SOLR_PORT!.port
            timeout /T 5
            REM Kill it if it is still running after the graceful shutdown
            For /f "tokens=5" %%M in ('netstat -nao ^| find /i "listening" ^| find ":!SOME_SOLR_PORT!"') do (taskkill /f /PID %%M)
          )
        )
      )
    ) ELSE (
      set SCRIPT_ERROR=Must specify the port when trying to stop Solr, or use -all to stop all running nodes on this host.
      goto err
    )
  ) ELSE (
    set found_it=0
    For /f "tokens=5" %%M in ('netstat -nao ^| find /i "listening" ^| find ":%SOLR_PORT%"') do (
      set found_it=1
      @echo Stopping Solr running on port %SOLR_PORT%
      set /A STOP_PORT=%SOLR_PORT% - 1000
      "%JAVA%" -jar "%SOLR_SERVER_DIR%\start.jar" STOP.PORT=!STOP_PORT! STOP.KEY=%STOP_KEY% --stop
      del %SOLR_TIP%\bin\solr-%SOLR_PORT%.port
      timeout /T 5
      REM Kill it if it is still running after the graceful shutdown
      For /f "tokens=5" %%j in ('netstat -nao ^| find /i "listening" ^| find ":%SOLR_PORT%"') do (taskkill /f /PID %%j)

      REM backup log files (use current timestamp for backup name)
      For /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c-%%a-%%b)
      For /f "tokens=1-2 delims=/:" %%a in ("%TIME%") do (set mytime=%%a%%b)
      set now_ts=!mydate!_!mytime!
      IF EXIST "%SOLR_SERVER_DIR%\logs\solr.log" (
        echo Backing up %SOLR_SERVER_DIR%\logs\solr.log
        move /Y "%SOLR_SERVER_DIR%\logs\solr.log" "%SOLR_SERVER_DIR%\logs\solr_log_!now_ts!"
      )

      IF EXIST "%SOLR_SERVER_DIR%\logs\solr_gc.log" (
        echo Backing up %SOLR_SERVER_DIR%\logs\solr_gc.log
        move /Y "%SOLR_SERVER_DIR%\logs\solr_gc.log" "%SOLR_SERVER_DIR%\logs\solr_gc_log_!now_ts!"
      )
    )
    if "!found_it!"=="0" echo No Solr found running on port %SOLR_PORT%
  )
  goto done
)

IF "%SOLR_PORT%"=="" set SOLR_PORT=8983
IF "%STOP_PORT%"=="" set /A STOP_PORT=%SOLR_PORT% - 1000

IF "%SCRIPT_CMD%"=="start" (
  REM see if Solr is already running using netstat
  For /f "tokens=5" %%j in ('netstat -aon ^| find /i "listening" ^| find ":%SOLR_PORT%"') do (
    set "SCRIPT_ERROR=Process %%j is already listening on port %SOLR_PORT%. If this is Solr, please stop it first before starting (or use restart). If this is not Solr, then please choose a different port using -p PORT"
    goto err
  )
)

REM if verbose gc logging enabled, setup the location of the log file
IF NOT "%GC_LOG_OPTS%"=="" set GC_LOG_OPTS=%GC_LOG_OPTS% -Xloggc:"%SOLR_SERVER_DIR%/logs/solr_gc.log"

IF "%SOLR_MODE%"=="solrcloud" (
  IF "%ZK_CLIENT_TIMEOUT%"=="" set "ZK_CLIENT_TIMEOUT=15000"

  set "CLOUD_MODE_OPTS=-DzkClientTimeout=!ZK_CLIENT_TIMEOUT!"

  IF NOT "%ZK_HOST%"=="" (
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -DzkHost=%ZK_HOST%"
  ) ELSE (
    IF "%verbose%"=="1" echo Configuring SolrCloud to launch an embedded ZooKeeper using -DzkRun
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -DzkRun"
  )
  IF EXIST "%SOLR_HOME%\collection1\core.properties" set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -Dbootstrap_confdir=./solr/collection1/conf -Dcollection.configName=myconf -DnumShards=1"
) ELSE (
  set CLOUD_MODE_OPTS=
)

REM These are useful for attaching remove profilers like VisualVM/JConsole
IF "%ENABLE_REMOTE_JMX_OPTS%"=="true" (
  set REMOTE_JMX_OPTS=-Dcom.sun.management.jmxremote ^
-Dcom.sun.management.jmxremote.local.only=false ^
-Dcom.sun.management.jmxremote.ssl=false ^
-Dcom.sun.management.jmxremote.authenticate=false ^
-Dcom.sun.management.jmxremote.port=10%SOLR_PORT:~-2,2% ^
-Dcom.sun.management.jmxremote.rmi.port=10%SOLR_PORT:~-2,2%

IF NOT "%SOLR_HOST%"=="" set REMOTE_JMX_OPTS=%REMOTE_JMX_OPTS% -Djava.rmi.server.hostname=%SOLR_HOST%
) ELSE (
  set REMOTE_JMX_OPTS=
)

IF NOT "%SOLR_HEAP%"=="" set SOLR_JAVA_MEM=-Xms%SOLR_HEAP% -Xmx%SOLR_HEAP%
IF "%SOLR_JAVA_MEM%"=="" set SOLR_JAVA_MEM=-Xms512m -Xmx512m
IF "%SOLR_TIMEZONE%"=="" set SOLR_TIMEZONE=UTC

IF "%verbose%"=="1" (
    @echo Starting Solr using the following settings:
    @echo     JAVA            = %JAVA%
    @echo     SOLR_SERVER_DIR = %SOLR_SERVER_DIR%
    @echo     SOLR_HOME       = %SOLR_HOME%
    @echo     SOLR_HOST       = %SOLR_HOST%
    @echo     SOLR_PORT       = %SOLR_PORT%
    @echo     GC_TUNE         = %GC_TUNE%
    @echo     GC_LOG_OPTS     = %GC_LOG_OPTS%
    @echo     SOLR_JAVA_MEM   = %SOLR_JAVA_MEM%
    @echo     REMOTE_JMX_OPTS = %REMOTE_JMX_OPTS%
    @echo     CLOUD_MODE_OPTS = %CLOUD_MODE_OPTS%
    @echo     SOLR_TIMEZONE   = %SOLR_TIMEZONE%
)

set START_OPTS=-Duser.timezone=%SOLR_TIMEZONE% -Djava.net.preferIPv4Stack=true -Dsolr.autoSoftCommit.maxTime=3000
set START_OPTS=%START_OPTS% %GC_TUNE% %GC_LOG_OPTS%
IF NOT "!CLOUD_MODE_OPTS!"=="" set START_OPTS=%START_OPTS% !CLOUD_MODE_OPTS!
IF NOT "%REMOTE_JMX_OPTS%"=="" set START_OPTS=%START_OPTS% %REMOTE_JMX_OPTS%
IF NOT "%SOLR_ADDL_ARGS%"=="" set START_OPTS=%START_OPTS% %SOLR_ADDL_ARGS%
IF NOT "%SOLR_HOST_ARG%"=="" set START_OPTS=%START_OPTS% %SOLR_HOST_ARG%

cd "%SOLR_SERVER_DIR%"
@echo.
@echo Starting Solr on port %SOLR_PORT% from %SOLR_SERVER_DIR%
@echo.
IF "%FG%"=="1" (
  REM run solr in the foreground
  title "Solr-%SOLR_PORT%"
  echo %SOLR_PORT%>%SOLR_TIP%\bin\solr-%SOLR_PORT%.port
  "%JAVA%" -server -Xss256k %SOLR_JAVA_MEM% %START_OPTS% -DSTOP.PORT=!STOP_PORT! -DSTOP.KEY=%STOP_KEY% ^
    -Djetty.port=%SOLR_PORT% -Dsolr.solr.home="%SOLR_HOME%" -Dsolr.install.dir="%SOLR_TIP%" -jar start.jar
) ELSE (
  START "Solr-%SOLR_PORT%" "%JAVA%" -server -Xss256k %SOLR_JAVA_MEM% %START_OPTS% -DSTOP.PORT=!STOP_PORT! -DSTOP.KEY=%STOP_KEY% ^
    -Djetty.port=%SOLR_PORT% -Dsolr.solr.home="%SOLR_HOME%" -Dsolr.install.dir="%SOLR_TIP%" -jar start.jar > "%SOLR_SERVER_DIR%\logs\solr-%SOLR_PORT%-console.log"
  echo %SOLR_PORT%>%SOLR_TIP%\bin\solr-%SOLR_PORT%.port
)


set CREATE_EXAMPLE_CONFIG=
IF "%EXAMPLE%"=="schemaless" (
  set CREATE_EXAMPLE_CONFIG=data_driven_schema_configs
)
IF "%EXAMPLE%"=="techproducts" (
  set CREATE_EXAMPLE_CONFIG=sample_techproducts_configs
)

IF NOT "!CREATE_EXAMPLE_CONFIG!"=="" (
  timeout /T 5
  IF "%SOLR_MODE%"=="solrcloud" (
    "%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
      -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
      org.apache.solr.util.SolrCLI create_collection -name !EXAMPLE! -shards 1 -replicationFactor 1 ^
      -config !CREATE_EXAMPLE_CONFIG! -configsetsDir "%SOLR_SERVER_DIR%\solr\configsets" -solrUrl http://localhost:%SOLR_PORT%/solr
  ) ELSE (
    set "CREATE_URL=http://localhost:%SOLR_PORT%/solr/admin/cores?action=CREATE&name=%EXAMPLE%&configSet=!CREATE_EXAMPLE_CONFIG!"
    @echo.
    @echo Creating new core using command:
    @echo !CREATE_URL!
    @echo.
    "%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
      -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
      org.apache.solr.util.SolrCLI api -get "!CREATE_URL!"
  )
)

IF "%EXAMPLE%"=="techproducts" (
  @echo.
  @echo Indexing tech product example docs from %SOLR_TIP%\example\exampledocs
  "%JAVA%" -Durl=http://localhost:%SOLR_PORT%/solr/%EXAMPLE%/update -jar %SOLR_TIP%/example/exampledocs/post.jar %SOLR_TIP%/example/exampledocs/*.xml
)

@echo.
IF NOT "%EXAMPLE%"=="" (
  @echo Solr %EXAMPLE% example launched successfully.
)
@echo Direct your Web browser to http://localhost:%SOLR_PORT%/solr to visit the Solr Admin UI
@echo.

goto done

:cloud_example_start
REM Launch interactive session to guide the user through the SolrCloud example

CLS
@echo.
@echo Welcome to the SolrCloud example
@echo.
@echo.

IF "%NO_USER_PROMPT%"=="1" (
  set CLOUD_NUM_NODES=2
  @echo Starting up %CLOUD_NUM_NODES% Solr nodes for your example SolrCloud cluster.
  goto start_cloud_nodes
) ELSE (
  @echo This interactive session will help you launch a SolrCloud cluster on your local workstation.
  @echo.
  SET /P "USER_INPUT=To begin, how many Solr nodes would you like to run in your local cluster (specify 1-4 nodes) [2]: "
  goto while_num_nodes_not_valid
)

:while_num_nodes_not_valid
IF "%USER_INPUT%"=="" set USER_INPUT=2
SET /A INPUT_AS_NUM=!USER_INPUT!*1
IF %INPUT_AS_NUM% GEQ 1 IF %INPUT_AS_NUM% LEQ 4 set CLOUD_NUM_NODES=%INPUT_AS_NUM%
IF NOT DEFINED CLOUD_NUM_NODES (
  SET USER_INPUT=
  SET /P "USER_INPUT=Please enter a number between 1 and 4 [2]: "
  goto while_num_nodes_not_valid
)
@echo Ok, let's start up %CLOUD_NUM_NODES% Solr nodes for your example SolrCloud cluster.

:start_cloud_nodes
for /l %%x in (1, 1, !CLOUD_NUM_NODES!) do (
  set USER_INPUT=
  set /A idx=%%x-1
  set DEF_PORT=8983
  IF %%x EQU 2 (
    set DEF_PORT=7574
  ) ELSE (
    IF %%x EQU 3 (
    set DEF_PORT=8984
    ) ELSE (
      IF %%x EQU 4 (
        set DEF_PORT=7575
      )
    )
  )

  IF "%NO_USER_PROMPT%"=="1" (
    set NODE_PORT=!DEF_PORT!
  ) ELSE (
    set /P "USER_INPUT=Please enter the port for node%%x [!DEF_PORT!]: "
    IF "!USER_INPUT!"=="" set USER_INPUT=!DEF_PORT!
    set NODE_PORT=!USER_INPUT!
    echo node%%x port: !NODE_PORT!
    @echo.
  )

  IF NOT EXIST "%SOLR_TIP%\node%%x" (
    @echo Cloning %DEFAULT_SERVER_DIR% into %SOLR_TIP%\node%%x
    xcopy /Q /E /I "%DEFAULT_SERVER_DIR%" "%SOLR_TIP%\node%%x"
  )

  IF NOT "!SOLR_HEAP!"=="" (
    set "DASHM=-m !SOLR_HEAP!"
  ) ELSE (
    set "DASHM="
  )

  IF %%x EQU 1 (
    set EXAMPLE=
    IF NOT "!ZK_HOST!"=="" (
      set "DASHZ=-z !ZK_HOST!"
    ) ELSE (
      set "DASHZ="
    )
    @echo Starting node1 on port !NODE_PORT! using command:
    @echo solr -cloud -p !NODE_PORT! -d node1 !DASHZ! !DASHM!
    START "Solr-!NODE_PORT!" "%SDIR%\solr" -f -cloud -p !NODE_PORT! -d node1 !DASHZ! !DASHM!
    set NODE1_PORT=!NODE_PORT!
    echo !NODE_PORT!>%SOLR_TIP%\bin\solr-!NODE_PORT!.port
  ) ELSE (
    IF "!ZK_HOST!"=="" (
      set /A ZK_PORT=!NODE1_PORT!+1000
      set "ZK_HOST=localhost:!ZK_PORT!"
    )
    @echo Starting node%%x on port !NODE_PORT! using command:
    @echo solr -cloud -p !NODE_PORT! -d node%%x -z !ZK_HOST! !DASHM!
    START "Solr-!NODE_PORT!" "%SDIR%\solr" -f -cloud -p !NODE_PORT! -d node%%x -z !ZK_HOST! !DASHM!
    echo !NODE_PORT!>%SOLR_TIP%\bin\solr-!NODE_PORT!.port
  )

  timeout /T 10
)

set USER_INPUT=
echo.
echo Now let's create a new collection for indexing documents in your %CLOUD_NUM_NODES%-node cluster.
IF "%NO_USER_PROMPT%"=="1" (
  set CLOUD_COLLECTION=gettingstarted
  set CLOUD_NUM_SHARDS=2
  set CLOUD_REPFACT=2
  set CLOUD_CONFIG=data_driven_schema_configs
  goto create_collection
) ELSE (
  goto get_create_collection_params
)

:get_create_collection_params
set /P "USER_INPUT=Please provide a name for your new collection: [gettingstarted] "
IF "!USER_INPUT!"=="" set USER_INPUT=gettingstarted
set CLOUD_COLLECTION=!USER_INPUT!
echo !CLOUD_COLLECTION!
set USER_INPUT=
echo.
set /P "USER_INPUT=How many shards would you like to split !CLOUD_COLLECTION! into? [2] "
IF "!USER_INPUT!"=="" set USER_INPUT=2
set CLOUD_NUM_SHARDS=!USER_INPUT!
echo !CLOUD_NUM_SHARDS!
set USER_INPUT=
echo.
set /P "USER_INPUT=How many replicas per shard would you like to create? [2] "
IF "!USER_INPUT!"=="" set USER_INPUT=2
set CLOUD_REPFACT=!USER_INPUT!
echo !CLOUD_REPFACT!
set USER_INPUT=
echo.
set /P "USER_INPUT=Please choose a configuration for the !CLOUD_COLLECTION! collection, available options are: basic_configs, data_driven_schema_configs, or sample_techproducts_configs [data_driven_schema_configs]"
IF "!USER_INPUT!"=="" set USER_INPUT=data_driven_schema_configs
set CLOUD_CONFIG=!USER_INPUT!
echo !CLOUD_CONFIG!
goto create_collection

:create_collection
"%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
  -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
  org.apache.solr.util.SolrCLI create_collection -name !CLOUD_COLLECTION! -shards !CLOUD_NUM_SHARDS! -replicationFactor !CLOUD_REPFACT! ^
  -config !CLOUD_CONFIG! -configsetsDir "%SOLR_SERVER_DIR%\solr\configsets" -zkHost %zk_host%

echo.
echo SolrCloud example is running, please visit http://localhost:%NODE1_PORT%/solr"
echo.

REM End of interactive cloud example
goto done


:get_info
REM Find all Java processes, correlate with those listening on a port
REM and then try to contact via that port using the status tool
for /f "usebackq" %%i in (`dir /b %SOLR_TIP\bin% ^| findstr /i "^solr-.*\.port$"`) do (
  set SOME_SOLR_PORT=
  For /F "Delims=" %%J In (%SOLR_TIP%\bin\%%i) do set SOME_SOLR_PORT=%%~J
  if NOT "!SOME_SOLR_PORT!"=="" (
    for /f "tokens=2,5" %%j in ('netstat -aon ^| find /i "listening" ^| find /i "!SOME_SOLR_PORT!"') do (
      for /f "delims=: tokens=1,2" %%x IN ("%%j") do (
        @echo.
        set has_info=1
        echo Found Solr process %%k running on port !SOME_SOLR_PORT!
        "%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
          -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
          org.apache.solr.util.SolrCLI status -solr http://localhost:!SOME_SOLR_PORT!/solr
        @echo.
      )
    )
  )
)
if NOT "!has_info!"=="1" echo No running Solr nodes found.
set has_info=
goto done

:parse_healthcheck_args
IF [%1]==[] goto run_healthcheck
IF "%1"=="-c" goto set_healthcheck_collection
IF "%1"=="-collection" goto set_healthcheck_collection
IF "%1"=="-z" goto set_healthcheck_zk
IF "%1"=="-zkhost" goto set_healthcheck_zk
IF "%1"=="-help" goto usage
IF "%1"=="-usage" goto usage
IF "%1"=="/?" goto usage
goto run_healthcheck

:set_healthcheck_collection
set HEALTHCHECK_COLLECTION=%~2
SHIFT
SHIFT
goto parse_healthcheck_args

:set_healthcheck_zk
set HEALTHCHECK_ZK_HOST=%~2
SHIFT
SHIFT
goto parse_healthcheck_args

:run_healthcheck
IF NOT DEFINED HEALTHCHECK_COLLECTION goto healthcheck_usage
IF NOT DEFINED HEALTHCHECK_ZK_HOST set "HEALTHCHECK_ZK_HOST=localhost:9983"
"%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
  -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
  org.apache.solr.util.SolrCLI healthcheck -collection !HEALTHCHECK_COLLECTION! -zkHost !HEALTHCHECK_ZK_HOST!
goto done

:parse_create_args
IF [%1]==[] goto run_create
IF "%1"=="-c" goto set_create_config
IF "%1"=="-configset" goto set_create_config
IF "%1"=="-n" goto set_create_name
IF "%1"=="-name" goto set_create_name
IF "%1"=="-shards" goto set_create_shards
IF "%1"=="-replicationFactor" goto set_create_rf
IF "%1"=="-help" goto usage
IF "%1"=="-usage" goto usage
IF "%1"=="/?" goto usage
goto run_create

:set_create_configset
set CREATE_CONFIGSET=%~2
SHIFT
SHIFT
goto parse_create_args

:set_create_name
set CREATE_NAME=%~2
SHIFT
SHIFT
goto parse_create_args

:set_create_shards
set CREATE_NUM_SHARDS=%~2
SHIFT
SHIFT
goto parse_create_args

:set_create_rf
set CREATE_REPFACT=%~2
SHIFT
SHIFT
goto parse_create_args

:run_create
IF "!CREATE_NAME!"=="" (
  set "SCRIPT_ERROR=Name (-n) is a required parameter for $SCRIPT_CMD"
  goto invalid_cmd_line
)
IF "!CREATE_CONFIGSET!"=="" set CREATE_CONFIGSET=data_driven_schema_configs
IF "!CREATE_NUM_SHARDS!"=="" set CREATE_NUM_SHARDS=1
IF "!CREATE_REPFACT!"=="" set CREATE_REPFACT=1

REM Find a port that Solr is running on
set CREATE_PORT=0
for /f "usebackq" %%i in (`dir /b %SOLR_TIP\bin% ^| findstr /i "^solr-.*\.port$"`) do (
  set SOME_SOLR_PORT=
  For /F "Delims=" %%J In (%SOLR_TIP%\bin\%%i) do set SOME_SOLR_PORT=%%~J
  if NOT "!SOME_SOLR_PORT!"=="" (
    for /f "tokens=2,5" %%j in ('netstat -aon ^| find /i "listening" ^| find /i "!SOME_SOLR_PORT!"') do (
      set CREATE_PORT=!SOME_SOLR_PORT!
    )
  )
)
if "!CREATE_PORT!" EQU "0" (
  set "SCRIPT_ERROR=Could not find a running Solr instance on this host!"
  goto err
)

@echo Found Solr node running on port !CREATE_PORT!

@REM Determine if the Solr node is in cloud or standalone server mode
"%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
  -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
  org.apache.solr.util.SolrCLI status -solr http://localhost:!CREATE_PORT!/solr > solr_status.txt
set /p VAR=<solr_status.txt
del solr_status.txt
if "!VAR!"=="!VAR:ZooKeeper=!" (
  @REM Not in CloudMode - create core
  if "%SCRIPT_CMD%"=="create_core" (
    set "CREATE_URL=http://localhost:!CREATE_PORT!/solr/admin/cores?action=CREATE&name=!CREATE_NAME!&configSet=!CREATE_CONFIGSET!"
    @echo.
    @echo Creating new core using command:
    @echo !CREATE_URL!
    @echo.
    "%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
      -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
      org.apache.solr.util.SolrCLI api -get "!CREATE_URL!"
  ) else (
    @echo.
    @echo ERROR: Solr running on port !CREATE_PORT! is running in standalone server mode, please use the create_core command instead.
    @echo %SCRIPT_CMD% can only be used when running in SolrCloud mode.
    @echo.
    goto done
  )
) ELSE (
  @REM In CloudMode - create collection
  if "%SCRIPT_CMD%"=="create_collection" (
    "%JAVA%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
      -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
      org.apache.solr.util.SolrCLI create_collection -name !CREATE_NAME! -shards !CREATE_NUM_SHARDS! -replicationFactor !CREATE_REPFACT! ^
      -config !CREATE_CONFIGSET! -configsetsDir "%SOLR_TIP%\server\solr\configsets" -solrUrl http://localhost:!CREATE_PORT!/solr
  ) else (
    @echo.
    @echo ERROR: Solr running on port !CREATE_PORT! is running in SolrCloud mode, please use the create_collection command instead.
    @echo %SCRIPT_CMD% can only be used when running in standalone server mode.
    @echo.
    goto done
  )
)
goto done

:invalid_cmd_line
@echo.
IF "!SCRIPT_ERROR!"=="" (
  @echo Invalid command-line option: %1
) ELSE (
  @echo ERROR: !SCRIPT_ERROR!
)
@echo.
IF "%FIRST_ARG%"=="start" (
  goto start_usage
) ELSE IF "%FIRST_ARG:~0,1%" == "-" (
  goto start_usage
) ELSE IF "%FIRST_ARG%"=="restart" (
  goto start_usage
) ELSE IF "%FIRST_ARG%"=="stop" (
  goto stop_usage
) ELSE IF "%FIRST_ARG%"=="healthcheck" (
  goto healthcheck_usage
) ELSE IF "%FIRST_ARG%"=="create_core" (
  goto create_core_usage
) ELSE IF "%FIRST_ARG%"=="create_collection" (
  goto create_collection_usage
) ELSE (
  goto script_usage
)

:need_java_home
@echo Please set the JAVA_HOME environment variable to the path where you installed Java 1.7+
goto done

:need_java_vers
@echo Java 1.7 or later is required to run Solr.
goto done

:err
@echo.
@echo ERROR: !SCRIPT_ERROR!
@echo.
exit /b 1

:done

ENDLOCAL
