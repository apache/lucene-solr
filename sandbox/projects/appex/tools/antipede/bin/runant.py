#!/usr/bin/python
"""

 runant.py

	This script is a translation of the runant.pl written by Steve Loughran.
	It runs ant with/out arguments, it should be quite portable (thanks to
	the python os library)
	This script has been tested with Python2.0/Win2K

        Copyright (c) 2001 The Apache Software Foundation.  All rights
        reserved.

 created:         2001-04-11
 author:          Pierre Dittgen pierre.dittgen@criltelecom.com

 Assumptions:

 - the "java" executable/script is on the command path
 - ANT_HOME has been set
"""
import os, os.path, string, sys

# Change it to 1 to get extra debug information
debug = 0

#######################################################################
#
# check to make sure environment is setup
#
if not os.environ.has_key('ANT_HOME'):
	print '\n\nANT_HOME *MUST* be set!\n\n'
	sys.exit(1)
else:
	ANT_HOME = os.environ['ANT_HOME']

if not os.environ.has_key('JAVACMD'):
	JAVACMD = 'java'
else:
	JAVACMD = os.environ['JAVACMD']

# Sets the separator char for CLASSPATH
SEPARATOR = ':'
if os.name == 'dos' or os.name == 'nt':
	SEPARATOR = ';'

# Build up standard classpath
localpath = ''
if os.environ.has_key('CLASSPATH'):
	localpath = os.environ['CLASSPATH']
else:
	if debug:
		print 'Warning: no initial classpath\n'

# Add jar files
LIBDIR = os.path.join(ANT_HOME, 'lib')
jarfiles = []
for file in os.listdir(LIBDIR):
	if file[-4:] == '.jar':
		jarfiles.append(os.path.join(LIBDIR,file))
if debug:
	print 'Jar files:'
	for jar in jarfiles:
		print jar
localpath = localpath + SEPARATOR + string.join(jarfiles, SEPARATOR)

# If JAVA_HOME is defined, look for tools.jar & classes.zip
# and add to classpath
if os.environ.has_key('JAVA_HOME') and os.environ['JAVA_HOME'] != '':
	JAVA_HOME = os.environ['JAVA_HOME']
	TOOLS = os.path.join(JAVA_HOME, os.path.join('lib', 'tools.jar'))
	if os.path.exists(TOOLS):
		localpath = localpath + SEPARATOR + TOOLS
	CLASSES = os.path.join(JAVA_HOME, os.path.join('lib', 'classes.zip'))
	if os.path.exists(CLASSES):
		localpath = localpath + SEPARATOR + CLASSES
else:
	print '\n\nWarning: JAVA_HOME environment variable is not set.\n', \
		'If the build fails because sun.* classes could not be found\n', \
		'you will need to set the JAVA_HOME environment variable\n', \
		'to the installation directory of java\n'

# Jikes
ANT_OPTS = []
if os.environ.has_key('ANT_OPTS'):
	ANT_OPTS = string.split(os.environ['ANT_OPTS'])
if os.environ.has_key('JIKESPATH'):
	ANT_OPTS.append('-Djikes.class.path=' + os.environ['JIKESPATH'])

# Builds the commandline
cmdline = '%s -classpath %s -Dant.home=%s %s org.apache.tools.ant.Main %s' \
	 % (JAVACMD, localpath, ANT_HOME, string.join(ANT_OPTS,' '), \
	 	string.join(sys.argv[1:], ' '))

if debug:
	print '\n%s\n\n' % (cmdline)

# Run the biniou!
os.system(cmdline)
