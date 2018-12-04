@echo off
@setlocal enabledelayedexpansion

set ADD_OPENS_OPTION=--add-opens java.base/java.lang=ALL-UNNAMED
java %ADD_OPENS_OPTION% -version > nul 2>&1
if %errorlevel% equ 0 (
    rem running on jdk9+
    set JAVA_OPTIONS=%ADD_OPENS_OPTION%
)

set JAVA_OPTIONS=%JAVA_OPTIONS% -Xmx1024m -Xms512m -XX:MaxMetaspaceSize=256m

set CLASSPATHS=.\*;.\lib\*;..\core\*;..\codecs\*;..\backward-codecs\*;..\queries\*;..\queryparser\*;..\misc\*
for /d %%A in (..\analysis\*) do (
    set "CLASSPATHS=!CLASSPATHS!;%%A\*;%%A\lib\*"
)

start javaw -cp %CLASSPATHS% %JAVA_OPTIONS% org.apache.lucene.luke.app.desktop.LukeMain
