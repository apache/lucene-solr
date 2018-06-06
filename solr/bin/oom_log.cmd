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

set SOLR_PID=%1
shift
set SOLR_PORT=%1
shift
set SOLR_LOGS_DIR=%1

set MyDate=%date:~10,4%-%date:~4,2%-%date:~7,2%
set MyTime=%time:~0,2%-%time:~3,2%-%time:~6,2%
if "%MyTime~0,1%"==" " set MyTime=0%MyTime:~1%
set NOW=%MyDate%_%MyTime%
set LOGFILE=%SOLR_LOGS_DIR%\solr_oom_log-%SOLR_PORT%-%NOW%.log

echo "Logging OOM event for Solr process $SOLR_PID, port $SOLR_PORT" > $LOGFILE 
echo "Process has been terminated" >> $LOGFILE
