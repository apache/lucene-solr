# GNU make rules for lucene

# determine whether we're on Win32 or Unix
ifeq ($(findstring CYGWIN,$(shell uname)),CYGWIN)
  OS = win32
else
  OS = unix
endif

# DOS compatibility:
# These should be used in variables that end up in CLASSPATH.
ifeq ($(OS),win32)
  SLASH=\\
  COLON=;
else
  SLASH=/
  COLON=:
endif

# ROOT should be set to the root directory of the Lucene package
# hierarchy.  This is typically ../../.., as most packages are of the
# form com.lucene.<package>.
ifeq ($(ROOT),)
  ROOT = ..$(SLASH)..$(SLASH)..
else
  ROOT := $(subst /,$(SLASH),$(ROOT))
endif

#include all the relevant variables
include $(subst $(SLASH),/,$(ROOT))/com/lucene/variables.mk

# directories containing java source code
DIRS = store util document analysis analysis/standard index search queryParser
PACKAGES = $(subst /,.,$(patsubst %,com.lucene.%,$(DIRS)))

ifeq ($(JDK_HOME),)
  ifneq ($(JAVA_HOME),)
     JDK_HOME=$(JAVA_HOME)
   else
     ifeq ($(OS),win32)
       JDK_HOME = C:/jdk1.3.1
     else
       JDK_HOME = /usr/local/java/jdk1.3.1
     endif
   endif
endif

# Location of JavaCC
ifeq ($(JAVACC),)
 ifeq ($(OS),win32)
  JAVACC = C:/javacc2_0/bin/lib/JavaCC.zip
 else
  JAVACC = /usr/local/java/javacc2_0/bin/lib/JavaCC.zip
 endif
endif

JAVADIR = $(subst \,/,$(JDK_HOME))

# The compiler executable.
ifeq ($(JAVAC),)
  JAVAC = $(JAVADIR)/bin/javac
endif

# The java executable
JAVA = $(JAVADIR)/bin/java

# The jar executable
JAR = $(JAVADIR)/bin/jar

# javadoc
JAVADOC = $(JAVADIR)/bin/javadoc

# Options to pass to Java compiler
ifeq ($(JFLAGS),)
  JFLAGS = -O
endif


# CLASSPATH
# By default include the Lucene root, and Java's builtin classes
ifeq ($(OLDJAVA),)
  export CLASSPATH=$(PREPENDCLASSPATH)$(COLON)$(ROOT)$(COLON)$(JDK_HOME)$(SLASH)jre$(SLASH)lib$(SLASH)rt.jar
else
  export CLASSPATH=$(PREPENDCLASSPATH)$(COLON)$(ROOT)$(COLON)$(JDK_HOME)$(SLASH)lib$(SLASH)classes.zip
endif

# JIKESPATH overrides the classpath variable for jikes, so we need to set it
# here to avoid problems with a jikes user
export JIKESPATH=$(CLASSPATH)

## Rules

# Use JAVAC to compile .java files into .class files
%.class : %.java
	$(JAVAC) $(JFLAGS) $<

# Compile .jj files to .java with JavaCC
%.java : %.jj
	$(JAVA) -classpath '$(CLASSPATH)$(COLON)$(JAVACC)' COM.sun.labs.javacc.Main $<

# Add JavaCC generated files to 'classes' and 'clean' targets.
JJFILES = $(wildcard *.jj)
ifneq ($(JJFILES),)
  CLASSES += $(patsubst %.jj,%.class,  $(JJFILES))
  DIRT += $(patsubst %.jj,%.java, $(JJFILES))
  DIRT += $(patsubst %.jj,%Constants.java, $(JJFILES))
  DIRT += $(patsubst %.jj,%TokenManager.java, $(JJFILES))
  DIRT += Token.java TokenMgrError.java TokenManager.java \
          CharStream.java ASCII_CharStream.java ParseException.java
endif


# Don't delete parser's .java file -- it's needed by javadoc.
.PRECIOUS: $(patsubst %.jj,%.java, $(JJFILES))


# Assume all .java files should have a .class file.
CLASSES += $(patsubst %.java,%.class,$(wildcard *.java))

# default rule
classes : $(CLASSES)

# Removes all generated files from the connected src directory.
clean:
	rm -f *.class $(DIRT)

# include all the rules for the root directory..
include $(subst $(SLASH),/,$(ROOT))/com/lucene/rootrules.mk
