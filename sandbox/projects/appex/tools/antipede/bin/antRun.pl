#!/usr/bin/perl
#######################################################################
#
# antRun.pl
#
# wrapper script for invoking commands on a platform with Perl installed
# this is akin to antRun.bat, and antRun the SH script 
#
# created:         2001-10-18
# last modified:   2001-11-13
# author:          Jeff Tulley jtulley@novell.com 
#######################################################################
#be fussy about variables
use strict;

#turn warnings on during dev; generates a few spurious uninitialised var access warnings
#use warnings;

#and set $debug to 1 to turn on trace info (currently unused)
my $debug=1;

#######################################################################
# change drive and directory to "%1"
my $ANT_RUN_CMD = @ARGV[0];

# assign current run command to "%2"
chdir (@ARGV[0]) || die "Can't cd to $ARGV[0]: $!\n";
if ($^O eq "NetWare") {
    # There is a bug in Perl 5 on NetWare, where chdir does not
    # do anything.  On NetWare, the following path-prefixed form should 
    # always work. (afaict)
    $ANT_RUN_CMD .= "/".@ARGV[1];
}
else {
    $ANT_RUN_CMD = @ARGV[1];
}

# dispose of the first two arguments, leaving only the command's args.
shift;
shift;

# run the command
my $returnValue = system $ANT_RUN_CMD, @ARGV;
if ($returnValue eq 0) {
    exit 0;
}
else {
    # only 0 and 1 are widely recognized as exit values
    # so change the exit value to 1
    exit 1;
}
