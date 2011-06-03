#!/usr/bin/perl
#
# poll-mirrors.pl
#
# This script is designed to poll download sites after posting a release
# and print out notice as each becomes available.  The RM can use this
# script to delay the release announcement until the release can be
# downloaded.
#
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

use strict;
use warnings;
use Getopt::Long;
use LWP::Simple;

my $version;
my $interval = 300;
my $quiet = 0;

my $result = GetOptions ("version=s" => \$version, "interval=i" => \$interval, "quiet" => \$quiet);

my $usage = "$0 -v version [ -i interval (seconds; default: 300)] [ -quiet ]";

unless ($result) {
  print STDERR $usage;
  exit(1);
}
unless (defined($version) && $version =~ /\d+(?:\.\d+)+/) {
  print STDERR "You must specify the release version.\n$usage";
  exit(1);
}

my $previously_selected = select STDOUT;
$| = 1; # turn off buffering of STDOUT, so "."s are printed immediately
select $previously_selected;

my $apache_backup_url = "http://www.apache.org/dist//lucene/java/$version/lucene-$version.tgz.asc";
my $maven_url = "http://repo2.maven.org/maven2/org/apache/lucene/lucene-core/$version/lucene-core-$version.pom";

my $apache_available = 0;
my $maven_available = 0;

until ($apache_available && $maven_available) {
  unless ($apache_available) {
    my $content = get($apache_backup_url);
    $apache_available = defined($content);
    print "\nDownloadable: $apache_backup_url\n" if ($apache_available);
  }
  unless ($maven_available) {
    my $content = get($maven_url);
    $maven_available = defined($content);
    print "\nDownloadable: $maven_url\n" if ($maven_available);
  }
  print "." unless ($quiet);
  sleep($interval) unless ($apache_available && $maven_available);
}
