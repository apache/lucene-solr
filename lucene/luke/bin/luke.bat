@echo off
@setlocal enabledelayedexpansion

cd /d %~dp0

set JAVA_OPTIONS=%JAVA_OPTIONS% -Xmx1024m -Xms512m -XX:MaxMetaspaceSize=256m

set CLASSPATHS=.\*;.\lib\*;..\core\*;..\codecs\*;..\backward-codecs\*;..\queries\*;..\queryparser\*;..\suggest\*;..\misc\*
for /d %%A in (..\analysis\*) do (
    set "CLASSPATHS=!CLASSPATHS!;%%A\*;%%A\lib\*"
)

start javaw -cp %CLASSPATHS% %JAVA_OPTIONS% org.apache.lucene.luke.app.desktop.LukeMain
