@echo off

REM   Copyright (c) 2001 The Apache Software Foundation.  All rights
REM   reserved.

rem Change drive and directory to %1 (Win9X only for NT/2K use "cd /d")
cd %1
%1\
set ANT_RUN_CMD=%2
shift
shift

set PARAMS=
:loop
if ""%1 == "" goto runCommand
set PARAMS=%PARAMS% %1
shift
goto loop

:runCommand
rem echo %ANT_RUN_CMD% %PARAMS%
%ANT_RUN_CMD% %PARAMS%

