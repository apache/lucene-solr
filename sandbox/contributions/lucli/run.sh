LUCLI=.
LUCLI_MEMORY=128M
#JAVA_HOME=/home/dror/j2sdk1.4.1_03/
CLASSPATH=${CLASSPATH}:$LUCLI/lib/libreadline-java.jar:$LUCLI/lib/lucene-1.3-rc3-dev.jar:$LUCLI/classes/lucli.jar
PATH=${PATH}:$JAVA_HOME/bin
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$LUCLI
export LD_LIBRARY_PATH
$JAVA_HOME/bin/java -Xmx${LUCLI_MEMORY} lucli.Lucli
#Use this line to enable tab completion. Depends on the Readline shares library
#$JAVA_HOME/bin/java lucli.Lucli -r

