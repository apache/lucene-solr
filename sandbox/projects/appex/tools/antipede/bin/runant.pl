#!/usr/bin/perl
#
#   Copyright (c) 2001 The Apache Software Foundation.  All rights
#   reserved.
#
#######################################################################
#
# runant.pl
#
# wrapper script for invoking ant in a platform with Perl installed
# this may include cgi-bin invocation, which is considered somewhat daft.
# (slo: that should be a separate file which can be derived from this
# and returns the XML formatted output)
#
# the code is not totally portable due to classpath and directory splitting
# issues. oops. (NB, use File::Spec::Functions  will help and the code is
# structured for the catfile() call, but because of perl version funnies
# the code is not included. 
#
# created:         2000-8-24
# last modified:   2000-8-24
# author:          Steve Loughran steve_l@sourceforge.net
#######################################################################
#
# Assumptions:
#
# - the "java" executable/script is on the command path
# - ANT_HOME has been set
# - target platform uses ":" as classpath separator or perl indicates it is dos/win32
# - target platform uses "/" as directory separator.

#be fussy about variables
use strict;

#platform specifics (disabled)
#use File::Spec::Functions;

#turn warnings on during dev; generates a few spurious uninitialised var access warnings
#use warnings;

#and set $debug to 1 to turn on trace info
my $debug=0;

#######################################################################
#
# check to make sure environment is setup
#

my $HOME = $ENV{ANT_HOME};
if ($HOME eq "")
        {
    die "\n\nANT_HOME *MUST* be set!\n\n";
        }

my $JAVACMD = $ENV{JAVACMD};
$JAVACMD = "java" if $JAVACMD eq "";

#ISSUE: what java wants to split up classpath varies from platform to platform 
#and perl is not too hot at hinting which box it is on.
#here I assume ":" 'cept on win32 and dos. Add extra tests here as needed.
my $s=":";
if(($^O eq "MSWin32") || ($^O eq "dos") || ($^O eq "cygwin"))
        {
        $s=";";
        }

#build up standard classpath
my $localpath=$ENV{CLASSPATH};
if ($localpath eq "")
        {
        print "warning: no initial classpath\n" if ($debug);
        $localpath="";
        }

#add jar files. I am sure there is a perl one liner to do this.
my $jarpattern="$HOME/lib/*.jar";
my @jarfiles =glob($jarpattern);
print "jarfiles=@jarfiles\n" if ($debug);
my $jar;
foreach $jar (@jarfiles )
        {
        $localpath.="$s$jar";
        }

#if Java home is defined, look for tools.jar & classes.zip and add to classpath
my $JAVA_HOME = $ENV{JAVA_HOME};
if ($JAVA_HOME ne "")
        {
        my $tools="$JAVA_HOME/lib/tools.jar";
        if (-e "$tools")
                {
                $localpath .= "$s$tools";
                }
        my $classes="$JAVA_HOME/lib/classes.zip";
        if (-e $classes)
                {
                $localpath .= "$s$classes";
                }
        }
else
        {
    print "\n\nWarning: JAVA_HOME environment variable is not set.\n".
                "If the build fails because sun.* classes could not be found\n".
                "you will need to set the JAVA_HOME environment variable\n".
                "to the installation directory of java\n";
        }

#set JVM options and Ant arguments, if any
my @ANT_OPTS=split(" ", $ENV{ANT_OPTS});
my @ANT_ARGS=split(" ", $ENV{ANT_ARGS});

#jikes
if($ENV{JIKESPATH} ne "")
        {
        push @ANT_OPTS, "-Djikes.class.path=$ENV{JIKESPATH}";
        }

#construct arguments to java
my @ARGS;
push @ARGS, "-classpath", "$localpath", "-Dant.home=$HOME";
push @ARGS, @ANT_OPTS;
push @ARGS, "org.apache.tools.ant.Main", @ANT_ARGS;
push @ARGS, @ARGV;

print "\n $JAVACMD @ARGS\n\n" if ($debug);

my $returnValue = system $JAVACMD, @ARGS;
if ($returnValue eq 0)
        {
        exit 0;
        }
else
        {
        # only 0 and 1 are widely recognized as exit values
        # so change the exit value to 1
        exit 1;
        }
