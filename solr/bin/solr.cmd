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

REM URL scheme for contacting Solr
set SOLR_URL_SCHEME=http
IF DEFINED SOLR_SSL_OPTS set SOLR_URL_SCHEME=https
IF NOT DEFINED SOLR_SSL_OPTS set SOLR_SSL_OPTS=

REM Verify Java is available
IF DEFINED SOLR_JAVA_HOME set "JAVA_HOME=%SOLR_JAVA_HOME%"
REM Try to detect JAVA_HOME from the registry
IF NOT DEFINED JAVA_HOME (
  FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment" /v CurrentVersion') DO set CurVer=%%B
  FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment\!CurVer!" /v JavaHome') DO (
    set JAVA_HOME=%%B
  )
)

IF NOT DEFINED JAVA_HOME goto need_java_home
set JAVA_HOME=%JAVA_HOME:"=%
"%JAVA_HOME%"\bin\java -version:1.8 -version > nul 2>&1
IF ERRORLEVEL 1 "%JAVA_HOME%"\bin\java -version:1.7 -version > nul 2>&1
IF ERRORLEVEL 1 goto need_java_vers
set "JAVA=%JAVA_HOME%\bin\java"

set "DEFAULT_SERVER_DIR=%SOLR_TIP%\server"

set FIRST_ARG=%1

IF [%1]==[] goto usage

IF "%1"=="-help" goto usage
IF "%1"=="-usage" goto usage
IF "%1"=="/?" goto usage
IF "%1"=="-i" goto get_info
IF "%1"=="-info" goto get_info
IF "%1"=="status" goto get_info

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
IF "%1"=="create" (
  set SCRIPT_CMD=create
  SHIFT
  goto parse_create_args
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
IF "%1"=="delete" (
  set SCRIPT_CMD=delete
  SHIFT
  goto parse_delete_args
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
IF "%SCRIPT_CMD%"=="create" goto create_usage
IF "%SCRIPT_CMD%"=="create_core" goto create_core_usage
IF "%SCRIPT_CMD%"=="create_collection" goto create_collection_usage
IF "%SCRIPT_CMD%"=="delete" goto delete_usage
goto done

:script_usage
@echo.
@echo Usage: solr COMMAND OPTIONS
@echo        where COMMAND is one of: start, stop, restart, healthcheck, create, create_core, create_collection, delete
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
@echo  -p port     Specify the port the Solr HTTP listener is bound to
@echo.
@echo  -all        Find and stop all running Solr servers on this host
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

:create_usage
echo.
echo Usage: solr create [-c name] [-d confdir] [-n confname] [-shards #] [-replicationFactor #] [-p port]
echo.
echo   Create a core or collection depending on whether Solr is running in standalone (core) or SolrCloud
echo   mode (collection). In other words, this action detects which mode Solr is running in, and then takes
echo   the appropriate action (either create_core or create_collection). For detailed usage instructions, do:
echo.
echo     bin\solr create_core -help
echo.
echo        or
echo.
echo     bin\solr create_collection -help
echo.
goto done

:delete_usage
echo.
echo Usage: solr delete [-c name] [-deleteConfig boolean] [-p port]
echo.
echo  Deletes a core or collection depending on whether Solr is running in standalone (core) or SolrCloud
echo  mode (collection). If you're deleting a collection in SolrCloud mode, the default behavior is to also
echo  delete the configuration directory from ZooKeeper so long as it is not being used by another collection.
echo  You can override this behavior by passing -deleteConfig false when running this command.
echo.
echo   -c name     Name of core to create
echo.
echo   -deleteConfig boolean Delete the configuration directory from ZooKeeper; default is true
echo.
echo   -p port     Port of a local Solr instance where you want to create the new core
echo                 If not specified, the script will search the local system for a running
echo                 Solr instance and will use the port of the first server it finds.
echo.
goto done

:create_core_usage
echo.
echo Usage: solr create_core [-c name] [-d confdir] [-p port]
echo.
echo   -c name     Name of core to create
echo.
echo   -d confdir  Configuration directory to copy when creating the new core, built-in options are:
echo.
echo       basic_configs: Minimal Solr configuration
echo       data_driven_schema_configs: Managed schema with field-guessing support enabled
echo       sample_techproducts_configs: Example configuration with many optional features enabled to
echo          demonstrate the full power of Solr
echo.
echo       If not specified, default is: data_driven_schema_configs
echo.
echo       Alternatively, you can pass the path to your own configuration directory instead of using
echo       one of the built-in configurations, such as: bin\solr create_core -c mycore -d c:/tmp/myconfig
echo.
echo   -p port     Port of a local Solr instance where you want to create the new core
echo                 If not specified, the script will search the local system for a running
echo                 Solr instance and will use the port of the first server it finds.
echo.
goto done

:create_collection_usage
echo.
echo Usage: solr create_collection [-c name] [-d confdir] [-n confname] [-shards #] [-replicationFactor #] [-p port]
echo.
echo   -c name               Name of collection to create
echo.
echo   -d confdir            Configuration directory to copy when creating the new collection, built-in options are:
echo.
echo       basic_configs: Minimal Solr configuration
echo       data_driven_schema_configs: Managed schema with field-guessing support enabled
echo       sample_techproducts_configs: Example configuration with many optional features enabled to
echo          demonstrate the full power of Solr
echo.
echo       If not specified, default is: data_driven_schema_configs
echo.
echo       Alternatively, you can pass the path to your own configuration directory instead of using
echo       one of the built-in configurations, such as: bin\solr create_collection -c mycoll -d c:/tmp/myconfig
echo.
echo       By default the script will upload the specified confdir directory into ZooKeeper using the same
echo         name as the collection (-c) option. Alternatively, if you want to reuse an existing directory
echo         or create a confdir in ZooKeeper that can be shared by multiple collections, use the -n option
echo.
echo   -n configName         Name the configuration directory in ZooKeeper; by default, the configuration
echo                             will be uploaded to ZooKeeper using the collection name (-c), but if you want
echo                             to use an existing directory or override the name of the configuration in
echo                              ZooKeeper, then use the -c option.
echo.
echo   -shards #             Number of shards to split the collection into
echo.
echo   -replicationFactor #  Number of copies of each document in the collection
echo.
echo   -p port               Port of a local Solr instance where you want to create the new collection
echo                           If not specified, the script will search the local system for a running
echo                           Solr instance and will use the port of the first server it finds.
echo.
goto done

REM Really basic command-line arg parsing
:parse_args

set "arg=%~1"
set "firstTwo=%arg:~0,2%"
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
IF "%firstTwo%"=="-D" goto set_passthru
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

:set_passthru
set "PASSTHRU=%~1=%~2"
IF NOT "%SOLR_OPTS%"=="" (
  set "SOLR_OPTS=%SOLR_OPTS% %PASSTHRU%"
) ELSE (
  set "SOLR_OPTS=%PASSTHRU%"
)
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

IF "%SOLR_SERVER_DIR%"=="" set SOLR_SERVER_DIR=%DEFAULT_SERVER_DIR%

IF NOT EXIST "%SOLR_SERVER_DIR%" (
  set SCRIPT_ERROR=Solr server directory %SOLR_SERVER_DIR% not found!
  goto err
)

IF "%EXAMPLE%"=="" (
  REM SOLR_HOME just becomes serverDir/solr
) ELSE IF "%EXAMPLE%"=="techproducts" (
  mkdir "%SOLR_TIP%\example\techproducts\solr"
  set "SOLR_HOME=%SOLR_TIP%\example\techproducts\solr"
  IF NOT EXIST "!SOLR_HOME!\solr.xml" (
    copy "%DEFAULT_SERVER_DIR%\solr\solr.xml" "!SOLR_HOME!\solr.xml"
  )
  IF NOT EXIST "!SOLR_HOME!\zoo.cfg" (
    copy "%DEFAULT_SERVER_DIR%\solr\zoo.cfg" "!SOLR_HOME!\zoo.cfg"
  )
) ELSE IF "%EXAMPLE%"=="cloud" (
  set SOLR_MODE=solrcloud
  goto cloud_example_start
) ELSE IF "%EXAMPLE%"=="dih" (
  set "SOLR_HOME=%SOLR_TIP%\example\example-DIH\solr"
) ELSE IF "%EXAMPLE%"=="schemaless" (
  mkdir "%SOLR_TIP%\example\schemaless\solr"
  set "SOLR_HOME=%SOLR_TIP%\example\schemaless\solr"
  IF NOT EXIST "!SOLR_HOME!\solr.xml" (
    copy "%DEFAULT_SERVER_DIR%\solr\solr.xml" "!SOLR_HOME!\solr.xml"
  )
  IF NOT EXIST "!SOLR_HOME!\zoo.cfg" (
    copy "%DEFAULT_SERVER_DIR%\solr\zoo.cfg" "!SOLR_HOME!\zoo.cfg"
  )
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
  ) ELSE IF EXIST "%cd%\%SOLR_HOME%" (
    set "SOLR_HOME=%cd%\%SOLR_HOME%"
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

@REM This is quite hacky, but examples rely on a different log4j.properties
@REM so that we can write logs for examples to %SOLR_HOME%\..\logs
set "SOLR_LOGS_DIR=%SOLR_SERVER_DIR%\logs"
set "EXAMPLE_DIR=%SOLR_TIP%\example"
set LOG4J_CONFIG=
set TMP=!SOLR_HOME:%EXAMPLE_DIR%=!
IF NOT "%TMP%"=="%SOLR_HOME%" (
  set "SOLR_LOGS_DIR=%SOLR_HOME%\..\logs"
  set "LOG4J_CONFIG=file:%EXAMPLE_DIR%\resources\log4j.properties"
)

@REM stop logic here
IF "%SCRIPT_CMD%"=="stop" (
  IF "%SOLR_PORT%"=="" (
    IF "%STOP_ALL%"=="1" (
      set found_it=0
      for /f "usebackq" %%i in (`dir /b "%SOLR_TIP%\bin" ^| findstr /i "^solr-.*\.port$"`) do (
        set SOME_SOLR_PORT=
        For /F "Delims=" %%J In ('type "%SOLR_TIP%\bin\%%i"') do set SOME_SOLR_PORT=%%~J
        if NOT "!SOME_SOLR_PORT!"=="" (
          for /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":!SOME_SOLR_PORT! "') do (
            @REM j is the ip:port and k is the pid
            IF NOT "%%k"=="0" (
              @REM split the ip:port var by colon to see if the ip is 0.0.0.0
              for /f "delims=: tokens=1,2" %%x IN ("%%j") do (
                @REM x is the ip
                IF "%%x"=="0.0.0.0" (
                  set found_it=1
                  @echo Stopping Solr process %%k running on port !SOME_SOLR_PORT!
                  set /A STOP_PORT=!SOME_SOLR_PORT! - 1000
                  "%JAVA%" %SOLR_SSL_OPTS% -Djetty.home="%SOLR_SERVER_DIR%" -jar "%SOLR_SERVER_DIR%\start.jar" STOP.PORT=!STOP_PORT! STOP.KEY=%STOP_KEY% --stop
                  del "%SOLR_TIP%"\bin\solr-!SOME_SOLR_PORT!.port
                  timeout /T 5
                  REM Kill it if it is still running after the graceful shutdown
                  For /f "tokens=2,5" %%M in ('netstat -nao ^| find "TCP " ^| find ":!SOME_SOLR_PORT! "') do (
                    IF "%%N"=="%%k" (
                      for /f "delims=: tokens=1,2" %%a IN ("%%M") do (
                        IF "%%a"=="0.0.0.0" (
                          @echo Forcefully killing process %%N
                          taskkill /f /PID %%N
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
      if "!found_it!"=="0" echo No Solr nodes found to stop.
    ) ELSE (
      set SCRIPT_ERROR=Must specify the port when trying to stop Solr, or use -all to stop all running nodes on this host.
      goto err
    )
  ) ELSE (
    set found_it=0
    For /f "tokens=2,5" %%M in ('netstat -nao ^| find "TCP " ^| find ":%SOLR_PORT% "') do (
      IF NOT "%%N"=="0" (
        for /f "delims=: tokens=1,2" %%x IN ("%%M") do (
          IF "%%x"=="0.0.0.0" (
            set found_it=1
            @echo Stopping Solr process %%N running on port %SOLR_PORT%
            set /A STOP_PORT=%SOLR_PORT% - 1000
            "%JAVA%" %SOLR_SSL_OPTS% -Djetty.home="%SOLR_SERVER_DIR%" -jar "%SOLR_SERVER_DIR%\start.jar" STOP.PORT=!STOP_PORT! STOP.KEY=%STOP_KEY% --stop
            del "%SOLR_TIP%"\bin\solr-%SOLR_PORT%.port
            timeout /T 5
            REM Kill it if it is still running after the graceful shutdown
            For /f "tokens=2,5" %%j in ('netstat -nao ^| find "TCP " ^| find ":%SOLR_PORT% "') do (
              IF "%%N"=="%%k" (
                for /f "delims=: tokens=1,2" %%a IN ("%%j") do (
                  IF "%%a"=="0.0.0.0" (
                    @echo Forcefully killing process %%N
                    taskkill /f /PID %%N
                  )
                )
              )
            )
          )
        )
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
  For /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":%SOLR_PORT% "') do (
    IF NOT "%%k"=="0" (
      for /f "delims=: tokens=1,2" %%x IN ("%%j") do (
        IF "%%x"=="0.0.0.0" (
          set "SCRIPT_ERROR=Process %%k is already listening on port %SOLR_PORT%. If this is Solr, please stop it first before starting (or use restart). If this is not Solr, then please choose a different port using -p PORT"
          goto err
        )
      )
    )
  )
)

REM backup log files (use current timestamp for backup name)
For /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c-%%a-%%b)
For /f "tokens=1-2 delims=/:" %%a in ("%TIME%") do (set mytime=%%a%%b)
set now_ts=!mydate!_!mytime!
IF EXIST "!SOLR_LOGS_DIR!\solr.log" (
  echo Backing up !SOLR_LOGS_DIR!\solr.log
  move /Y "!SOLR_LOGS_DIR!\solr.log" "!SOLR_LOGS_DIR!\solr_log_!now_ts!"
)

IF EXIST "!SOLR_LOGS_DIR!\solr_gc.log" (
  echo Backing up !SOLR_LOGS_DIR!\solr_gc.log
  move /Y "!SOLR_LOGS_DIR!\solr_gc.log" "!SOLR_LOGS_DIR!\solr_gc_log_!now_ts!"
)

REM if verbose gc logging enabled, setup the location of the log file
IF NOT "%GC_LOG_OPTS%"=="" set GC_LOG_OPTS=%GC_LOG_OPTS% -Xloggc:"!SOLR_LOGS_DIR!/solr_gc.log"

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
  IF "!RMI_PORT!"=="" set RMI_PORT=1%SOLR_PORT%
  set REMOTE_JMX_OPTS=-Dcom.sun.management.jmxremote ^
-Dcom.sun.management.jmxremote.local.only=false ^
-Dcom.sun.management.jmxremote.ssl=false ^
-Dcom.sun.management.jmxremote.authenticate=false ^
-Dcom.sun.management.jmxremote.port=!RMI_PORT! ^
-Dcom.sun.management.jmxremote.rmi.port=!RMI_PORT!

IF NOT "%SOLR_HOST%"=="" set REMOTE_JMX_OPTS=%REMOTE_JMX_OPTS% -Djava.rmi.server.hostname=%SOLR_HOST%
) ELSE (
  set REMOTE_JMX_OPTS=
)

IF NOT "%SOLR_HEAP%"=="" set SOLR_JAVA_MEM=-Xms%SOLR_HEAP% -Xmx%SOLR_HEAP%
IF "%SOLR_JAVA_MEM%"=="" set SOLR_JAVA_MEM=-Xms512m -Xmx512m
IF "%SOLR_TIMEZONE%"=="" set SOLR_TIMEZONE=UTC

@REM Add Java version specific flags if needed
set JAVAVER=
set JAVA_MAJOR=
set JAVA_BUILD=0

"%JAVA%" -version 2>&1 | findstr /i "version" > javavers
set /p JAVAVEROUT=<javavers
del javavers
for /f "tokens=3" %%g in ("!JAVAVEROUT!") do (
  set JAVAVER=%%g
  set JAVAVER=!JAVAVER:"=!
  for /f "delims=_ tokens=1-3" %%v in ("!JAVAVER!") do (
    set JAVA_MAJOR=!JAVAVER:~0,3!
    set /a JAVA_BUILD=%%w
  )
)
IF "!JAVA_MAJOR!"=="1.7" (
  set "GC_TUNE=%GC_TUNE% -XX:CMSFullGCsBeforeCompaction=1 -XX:CMSTriggerPermRatio=80"
  IF !JAVA_BUILD! GEQ 40 (
    IF !JAVA_BUILD! LEQ 51 (
      set "GC_TUNE=!GC_TUNE! -XX:-UseSuperWord"
      @echo WARNING: Java version !JAVAVER! has known bugs with Lucene and requires the -XX:-UseSuperWord flag. Please consider upgrading your JVM.
    )
  )
)

IF "%verbose%"=="1" (
    @echo Starting Solr using the following settings:
    @echo     JAVA            = %JAVA%
    @echo     SOLR_SERVER_DIR = %SOLR_SERVER_DIR%
    @echo     SOLR_HOME       = %SOLR_HOME%
    @echo     SOLR_HOST       = %SOLR_HOST%
    @echo     SOLR_PORT       = %SOLR_PORT%
    @echo     STOP_PORT       = %STOP_PORT%
    @echo     SOLR_JAVA_MEM   = %SOLR_JAVA_MEM%
    @echo     GC_TUNE         = !GC_TUNE!
    @echo     GC_LOG_OPTS     = %GC_LOG_OPTS%
    @echo     SOLR_TIMEZONE   = %SOLR_TIMEZONE%

    IF "%SOLR_MODE%"=="solrcloud" (
      @echo     CLOUD_MODE_OPTS = %CLOUD_MODE_OPTS%
    )

    IF NOT "%SOLR_OPTS%"=="" (
      @echo     SOLR_OPTS       = %SOLR_OPTS%
    )

    IF NOT "%SOLR_ADDL_ARGS%"=="" (
      @echo     SOLR_ADDL_ARGS  = %SOLR_ADDL_ARGS%
    )

    IF "%ENABLE_REMOTE_JMX_OPTS%"=="true" (
        @echo     RMI_PORT        = !RMI_PORT!
        @echo     REMOTE_JMX_OPTS = %REMOTE_JMX_OPTS%
    )
)

set START_OPTS=-Duser.timezone=%SOLR_TIMEZONE% -Djava.net.preferIPv4Stack=true
set START_OPTS=%START_OPTS% !GC_TUNE! %GC_LOG_OPTS%
IF NOT "!CLOUD_MODE_OPTS!"=="" set "START_OPTS=%START_OPTS% !CLOUD_MODE_OPTS!"
IF NOT "%REMOTE_JMX_OPTS%"=="" set "START_OPTS=%START_OPTS% %REMOTE_JMX_OPTS%"
IF NOT "%SOLR_ADDL_ARGS%"=="" set "START_OPTS=%START_OPTS% %SOLR_ADDL_ARGS%"
IF NOT "%SOLR_HOST_ARG%"=="" set "START_OPTS=%START_OPTS% %SOLR_HOST_ARG%"
IF NOT "%SOLR_OPTS%"=="" set "START_OPTS=%START_OPTS% %SOLR_OPTS%"
IF NOT "%SOLR_SSL_OPTS%"=="" (
  set "SSL_PORT_PROP=-Djetty.ssl.port=%SOLR_PORT%"
  IF DEFINED SOLR_SSL_PORT set "SSL_PORT_PROP=-Djetty.ssl.port=%SOLR_SSL_PORT%"
  set "START_OPTS=%START_OPTS% %SOLR_SSL_OPTS% !SSL_PORT_PROP!"
)

IF NOT DEFINED LOG4J_CONFIG set "LOG4J_CONFIG=file:%SOLR_SERVER_DIR%\resources\log4j.properties"

cd "%SOLR_SERVER_DIR%"

IF NOT EXIST "!SOLR_LOGS_DIR!" (
  mkdir "!SOLR_LOGS_DIR!"
)

IF NOT EXIST "%SOLR_SERVER_DIR%\tmp" (
  mkdir "%SOLR_SERVER_DIR%\tmp"
)

@echo.
@echo Starting Solr on port %SOLR_PORT% from %SOLR_SERVER_DIR%
@echo.
IF "%FG%"=="1" (
  REM run solr in the foreground
  title "Solr-%SOLR_PORT%"
  echo %SOLR_PORT%>"%SOLR_TIP%"\bin\solr-%SOLR_PORT%.port
  "%JAVA%" -server -Xss256k %SOLR_JAVA_MEM% %START_OPTS% -Dlog4j.configuration="%LOG4J_CONFIG%" -DSTOP.PORT=!STOP_PORT! -DSTOP.KEY=%STOP_KEY% ^
    -Djetty.port=%SOLR_PORT% -Dsolr.solr.home="%SOLR_HOME%" -Dsolr.install.dir="%SOLR_TIP%" -Djetty.home="%SOLR_SERVER_DIR%" -Djava.io.tmpdir="%SOLR_SERVER_DIR%\tmp" -jar start.jar
) ELSE (
  START /B "Solr-%SOLR_PORT%" /D "%SOLR_SERVER_DIR%" "%JAVA%" -server -Xss256k %SOLR_JAVA_MEM% %START_OPTS% -Dlog4j.configuration="%LOG4J_CONFIG%" -DSTOP.PORT=!STOP_PORT! -DSTOP.KEY=%STOP_KEY% ^
    -Djetty.port=%SOLR_PORT% -Dsolr.solr.home="%SOLR_HOME%" -Dsolr.install.dir="%SOLR_TIP%" -Djetty.home="%SOLR_SERVER_DIR%" -Djava.io.tmpdir="%SOLR_SERVER_DIR%\tmp" -jar start.jar > "!SOLR_LOGS_DIR!\solr-%SOLR_PORT%-console.log"
  echo %SOLR_PORT%>"%SOLR_TIP%"\bin\solr-%SOLR_PORT%.port
)


set EXAMPLE_NAME=%EXAMPLE%
set CREATE_EXAMPLE_CONFIG=
IF "%EXAMPLE%"=="schemaless" (
  set EXAMPLE_NAME=gettingstarted
  set CREATE_EXAMPLE_CONFIG=data_driven_schema_configs
)
IF "%EXAMPLE%"=="techproducts" (
  set CREATE_EXAMPLE_CONFIG=sample_techproducts_configs
)

IF NOT "!CREATE_EXAMPLE_CONFIG!"=="" (
  timeout /T 10
  IF "%SOLR_MODE%"=="solrcloud" (
    "%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
      -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
      org.apache.solr.util.SolrCLI create_collection -name !EXAMPLE_NAME! -shards 1 -replicationFactor 1 ^
      -confdir !CREATE_EXAMPLE_CONFIG! -configsetsDir "%SOLR_SERVER_DIR%\solr\configsets" -solrUrl !SOLR_URL_SCHEME!://localhost:%SOLR_PORT%/solr
  ) ELSE (
    "%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
      -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
      org.apache.solr.util.SolrCLI create_core -name !EXAMPLE_NAME! -solrUrl !SOLR_URL_SCHEME!://localhost:%SOLR_PORT%/solr ^
      -confdir !CREATE_EXAMPLE_CONFIG! -configsetsDir "%SOLR_SERVER_DIR%\solr\configsets"
  )
)

IF "%EXAMPLE%"=="techproducts" (
  @echo.
  @echo Indexing tech product example docs from %SOLR_TIP%\example\exampledocs
  "%JAVA%" %SOLR_SSL_OPTS% -Durl=!SOLR_URL_SCHEME!://localhost:%SOLR_PORT%/solr/%EXAMPLE%/update -jar "%SOLR_TIP%/example/exampledocs/post.jar" "%SOLR_TIP%/example/exampledocs/*.xml"
)

@echo.
IF NOT "%EXAMPLE%"=="" (
  @echo Solr %EXAMPLE% example launched successfully.
)
@echo Direct your Web browser to !SOLR_URL_SCHEME!://localhost:%SOLR_PORT%/solr to visit the Solr Admin UI
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

set "CLOUD_EXAMPLE_DIR=%SOLR_TIP%\example\cloud"

@echo Creating Solr home %CLOUD_EXAMPLE_DIR%\node1\solr
mkdir "%CLOUD_EXAMPLE_DIR%\node1\solr"
copy "%DEFAULT_SERVER_DIR%\solr\solr.xml" "%CLOUD_EXAMPLE_DIR%\node1\solr\solr.xml"
copy "%DEFAULT_SERVER_DIR%\solr\zoo.cfg" "%CLOUD_EXAMPLE_DIR%\node1\solr\zoo.cfg"

for /l %%x in (2, 1, !CLOUD_NUM_NODES!) do (
  IF NOT EXIST "%SOLR_TIP%\node%%x" (
    @echo Cloning %CLOUD_EXAMPLE_DIR%\node1 into %CLOUD_EXAMPLE_DIR%\node%%x
    xcopy /Q /E /I "%CLOUD_EXAMPLE_DIR%\node1" "%CLOUD_EXAMPLE_DIR%\node%%x"
  )
)

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
    @echo solr -cloud -p !NODE_PORT! -s example\node1\solr !DASHZ! !DASHM!
    START "Solr-!NODE_PORT!" /D "%SDIR%" solr -f -cloud -p !NODE_PORT! !DASHZ! !DASHM! -s "%CLOUD_EXAMPLE_DIR%\node1\solr"
    set NODE1_PORT=!NODE_PORT!
    echo !NODE_PORT!>"%SOLR_TIP%"\bin\solr-!NODE_PORT!.port
  ) ELSE (
    IF "!ZK_HOST!"=="" (
      set /A ZK_PORT=!NODE1_PORT!+1000
      set "ZK_HOST=localhost:!ZK_PORT!"
    )
    @echo Starting node%%x on port !NODE_PORT! using command:
    @echo solr -cloud -p !NODE_PORT! -s example\node%%x\solr -z !ZK_HOST! !DASHM!
    START "Solr-!NODE_PORT!" /D "%SDIR%" solr -f -cloud -p !NODE_PORT! -z !ZK_HOST! !DASHM! -s "%CLOUD_EXAMPLE_DIR%\node%%x\solr"
    echo !NODE_PORT!>"%SOLR_TIP%"\bin\solr-!NODE_PORT!.port
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
"%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
  -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
  org.apache.solr.util.SolrCLI create_collection -name !CLOUD_COLLECTION! -shards !CLOUD_NUM_SHARDS! -replicationFactor !CLOUD_REPFACT! ^
  -confdir !CLOUD_CONFIG! -configsetsDir "%SOLR_SERVER_DIR%\solr\configsets" -zkHost %zk_host%

echo.
echo SolrCloud example is running, please visit !SOLR_URL_SCHEME!://localhost:%NODE1_PORT%/solr"
echo.

REM End of interactive cloud example
goto done


:get_info
REM Find all Java processes, correlate with those listening on a port
REM and then try to contact via that port using the status tool
for /f "usebackq" %%i in (`dir /b "%SOLR_TIP%\bin" ^| findstr /i "^solr-.*\.port$"`) do (
  set SOME_SOLR_PORT=
  For /F "Delims=" %%J In ('type "%SOLR_TIP%\bin\%%i"') do set SOME_SOLR_PORT=%%~J
  if NOT "!SOME_SOLR_PORT!"=="" (
    for /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":!SOME_SOLR_PORT! "') do (
      IF NOT "%%k"=="0" (
        for /f "delims=: tokens=1,2" %%x IN ("%%j") do (
          if "%%x"=="0.0.0.0" (
            @echo.
            set has_info=1
            echo Found Solr process %%k running on port !SOME_SOLR_PORT!
            "%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
              -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
              org.apache.solr.util.SolrCLI status -solr !SOLR_URL_SCHEME!://localhost:!SOME_SOLR_PORT!/solr
            @echo.
          )
        )
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
"%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
  -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
  org.apache.solr.util.SolrCLI healthcheck -collection !HEALTHCHECK_COLLECTION! -zkHost !HEALTHCHECK_ZK_HOST!
goto done

:parse_create_args
IF [%1]==[] goto run_create
IF "%1"=="-c" goto set_create_name
IF "%1"=="-core" goto set_create_name
IF "%1"=="-collection" goto set_create_name
IF "%1"=="-d" goto set_create_confdir
IF "%1"=="-confdir" goto set_create_confdir
IF "%1"=="-n" goto set_create_confname
IF "%1"=="-confname" goto set_create_confname
IF "%1"=="-s" goto set_create_shards
IF "%1"=="-shards" goto set_create_shards
IF "%1"=="-rf" goto set_create_rf
IF "%1"=="-replicationFactor" goto set_create_rf
IF "%1"=="-p" goto set_create_port
IF "%1"=="-port" goto set_create_port
IF "%1"=="-help" goto usage
IF "%1"=="-usage" goto usage
IF "%1"=="/?" goto usage
goto run_create

:set_create_name
set CREATE_NAME=%~2
SHIFT
SHIFT
goto parse_create_args

:set_create_confdir
set CREATE_CONFDIR=%~2
SHIFT
SHIFT
goto parse_create_args

:set_create_confname
set CREATE_CONFNAME=%~2
SHIFT
SHIFT
goto parse_create_args

:set_create_port
set CREATE_PORT=%~2
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
  set "SCRIPT_ERROR=Name (-c) is a required parameter for %SCRIPT_CMD%"
  goto invalid_cmd_line
)
IF "!CREATE_CONFDIR!"=="" set CREATE_CONFDIR=data_driven_schema_configs
IF "!CREATE_NUM_SHARDS!"=="" set CREATE_NUM_SHARDS=1
IF "!CREATE_REPFACT!"=="" set CREATE_REPFACT=1
IF "!CREATE_CONFNAME!"=="" set CREATE_CONFNAME=!CREATE_NAME!

REM Find a port that Solr is running on
if "!CREATE_PORT!"=="" (
  for /f "usebackq" %%i in (`dir /b "%SOLR_TIP%\bin" ^| findstr /i "^solr-.*\.port$"`) do (
    set SOME_SOLR_PORT=
    For /F "Delims=" %%J In ('type "%SOLR_TIP%\bin\%%i"') do set SOME_SOLR_PORT=%%~J
    if NOT "!SOME_SOLR_PORT!"=="" (
      for /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":!SOME_SOLR_PORT! "') do (
        IF NOT "%%k"=="0" set CREATE_PORT=!SOME_SOLR_PORT!
      )
    )
  )
)
if "!CREATE_PORT!"=="" (
  set "SCRIPT_ERROR=Could not find a running Solr instance on this host! Please use the -p option to specify the port."
  goto err
)

if "%SCRIPT_CMD%"=="create_core" (
  "%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
    -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
    org.apache.solr.util.SolrCLI create_core -name !CREATE_NAME! -solrUrl !SOLR_URL_SCHEME!://localhost:!CREATE_PORT!/solr ^
    -confdir !CREATE_CONFDIR! -configsetsDir "%SOLR_TIP%\server\solr\configsets"
) else (
  "%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
  -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
  org.apache.solr.util.SolrCLI create -name !CREATE_NAME! -shards !CREATE_NUM_SHARDS! -replicationFactor !CREATE_REPFACT! ^
  -confname !CREATE_CONFNAME! -confdir !CREATE_CONFDIR! -configsetsDir "%SOLR_TIP%\server\solr\configsets" -solrUrl !SOLR_URL_SCHEME!://localhost:!CREATE_PORT!/solr
)

goto done

:parse_delete_args
IF [%1]==[] goto run_delete
IF "%1"=="-c" goto set_delete_name
IF "%1"=="-core" goto set_delete_name
IF "%1"=="-collection" goto set_delete_name
IF "%1"=="-p" goto set_delete_port
IF "%1"=="-port" goto set_delete_port
IF "%1"=="-deleteConfig" goto set_delete_config
IF "%1"=="-help" goto usage
IF "%1"=="-usage" goto usage
IF "%1"=="/?" goto usage
goto run_delete

:set_delete_name
set DELETE_NAME=%~2
SHIFT
SHIFT
goto parse_delete_args

:set_delete_port
set DELETE_PORT=%~2
SHIFT
SHIFT
goto parse_delete_args

:set_delete_config
set DELETE_CONFIG=%~2
SHIFT
SHIFT
goto parse_delete_args

:run_delete
IF "!DELETE_NAME!"=="" (
  set "SCRIPT_ERROR=Name (-c) is a required parameter for %SCRIPT_CMD%"
  goto invalid_cmd_line
)

REM Find a port that Solr is running on
if "!DELETE_PORT!"=="" (
  for /f "usebackq" %%i in (`dir /b "%SOLR_TIP%\bin" ^| findstr /i "^solr-.*\.port$"`) do (
    set SOME_SOLR_PORT=
    For /F "Delims=" %%J In ('type "%SOLR_TIP%\bin\%%i"') do set SOME_SOLR_PORT=%%~J
    if NOT "!SOME_SOLR_PORT!"=="" (
      for /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":!SOME_SOLR_PORT! "') do (
        IF NOT "%%k"=="0" set DELETE_PORT=!SOME_SOLR_PORT!
      )
    )
  )
)
if "!DELETE_PORT!"=="" (
  set "SCRIPT_ERROR=Could not find a running Solr instance on this host! Please use the -p option to specify the port."
  goto err
)

if "!DELETE_CONFIG!"=="" (
  set DELETE_CONFIG=true
)

"%JAVA%" %SOLR_SSL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dlog4j.configuration="file:%DEFAULT_SERVER_DIR%\scripts\cloud-scripts\log4j.properties" ^
-classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
org.apache.solr.util.SolrCLI delete -name !DELETE_NAME! -deleteConfig !DELETE_CONFIG! ^
-solrUrl !SOLR_URL_SCHEME!://localhost:!DELETE_PORT!/solr

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
) ELSE IF "%FIRST_ARG%"=="create" (
  goto create_usage
) ELSE IF "%FIRST_ARG%"=="create_core" (
  goto create_core_usage
) ELSE IF "%FIRST_ARG%"=="create_collection" (
  goto create_collection_usage
) ELSE (
  goto script_usage
)

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
