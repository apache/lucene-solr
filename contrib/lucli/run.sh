LUCLI=.
LUCLI_MEMORY=128M
#JAVA_HOME=/home/dror/j2sdk1.4.1_03/
CLASSPATH=${CLASSPATH}:$LUCLI/lib/jline.jar:$LUCLI/lib/lucene.jar:$LUCLI/dist/lucli-dev.jar
export CLASSPATH
$JAVA_HOME/bin/java -Xmx${LUCLI_MEMORY} lucli.Lucli
