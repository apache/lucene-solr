#!/bin/sh

if [ "$JAVA_HOME" = "" ] ; then
  echo You must set JAVA_HOME to point at your Java Development Kit directory
  exit 1
fi

# convert the existing path to unix
if [ "$OSTYPE" = "cygwin32" ] || [ "$OSTYPE" = "cygwin" ] ; then
   CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# Add in your .jar files first
for i in ./lib/*.jar
do
    CLASSPATH=$CLASSPATH:"$i"
done
# Add in the jakarta-site2 library files
for i in ../jakarta-site2/lib/*.jar
do
    CLASSPATH=$CLASSPATH:"$i"
done

# convert the unix path to windows
if [ "$OSTYPE" = "cygwin32" ] || [ "$OSTYPE" = "cygwin" ] ; then
   CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
fi

BUILDFILE=build.xml

#echo $CLASSPATH

java $ANT_OPTS -classpath "$CLASSPATH" org.apache.tools.ant.Main \
                -Dant.home=$ANT_HOME \
                -buildfile ${BUILDFILE} \
                 "$@"
