REM   Copyright (c) 2001 The Apache Software Foundation.  All rights
REM   reserved.

set _CLASSPATHCOMPONENT=%1
:argCheck
if %2a==a goto gotAllArgs
shift
set _CLASSPATHCOMPONENT=%_CLASSPATHCOMPONENT% %1
goto argCheck
:gotAllArgs
set LOCALCLASSPATH=%_CLASSPATHCOMPONENT%;%LOCALCLASSPATH%

