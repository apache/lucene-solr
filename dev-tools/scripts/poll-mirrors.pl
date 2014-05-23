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
use POSIX qw/strftime/;
use LWP::UserAgent;

my $rel_path;
my $version;
my $interval = 300;
my $details = 0;

my $result = GetOptions ("version=s" => \$version,
                         "details!" => \$details,
                         "path=s" => \$rel_path,
                         "interval=i" => \$interval);

my $usage = ""
    . "$0 -v version [ -i interval (seconds; default: 300) ] [ -details ]\n"
    . "$0 -p some/explicit/path [ -i interval (seconds; default: 300) ] [ -details ]\n"
    ;

unless ($result) {
  print STDERR $usage;
  exit(1);
}

unless (defined($version) xor defined($rel_path)) {
  print STDERR "You must specify either -version or -path but not both\n$usage";
  exit(1);
}

my $label;
my $apache_url_suffix;
my $maven_url;

if (defined($version)) {
  if ($version !~ /^\d+(?:\.\d+)+/) {
    print STDERR "You must specify the release version as a number.\n$usage";
    exit(1);
  }
  $label = $version;
  $apache_url_suffix = "lucene/java/$version/changes/Changes.html";
  $maven_url = "http://repo1.maven.org/maven2/org/apache/lucene/lucene-core/$version/lucene-core-$version.pom.asc";
} else {
  # path based
  $apache_url_suffix = $label = $rel_path;
}
my $previously_selected = select STDOUT;
$| = 1; # turn off buffering of STDOUT, so status is printed immediately
select $previously_selected;

my $apache_mirrors_list_url = "http://www.apache.org/mirrors/";

my $agent = LWP::UserAgent->new();
$agent->timeout(2);

my $maven_available = defined($maven_url) ? 0 : -999;

my @apache_mirrors = ();

my $apache_mirrors_list_page = $agent->get($apache_mirrors_list_url)->decoded_content;
if (defined($apache_mirrors_list_page)) {
  #<TR>
  #  <TD ALIGN=RIGHT><A HREF="http://apache.dattatec.com/">apache.dattatec.com</A>&nbsp;&nbsp;<A HREF="http://apache.dattatec.com/">@</A></TD>
  #
  #  <TD>http</TD>
  #  <TD ALIGN=RIGHT>8 hours<BR><IMG BORDER=1 SRC="icons/mms14.gif" ALT=""></TD>
  #  <TD ALIGN=RIGHT>5 hours<BR><IMG BORDER=1 SRC="icons/mms14.gif" ALT=""></TD>
  #  <TD>ok</TD>
  #</TR>
  while ($apache_mirrors_list_page =~ m~<TR>(.*?)</TR>~gis) {
    my $mirror_entry = $1;
    next unless ($mirror_entry =~ m~<TD>\s*ok\s*</TD>\s*$~i); # skip mirrors with problems
    if ($mirror_entry =~ m~<A\s+HREF\s*=\s*"([^"]+)"\s*>~i) {
      my $mirror_url = $1;
      push @apache_mirrors, "${mirror_url}${apache_url_suffix}";
    }
  }
} else {
  print STDERR "Error fetching Apache mirrors list $apache_mirrors_list_url";
  exit(1);
}

my $num_apache_mirrors = $#apache_mirrors;

my $sleep_interval = 0;
while (1) {
  print "\n", strftime('%d-%b-%Y %H:%M:%S', localtime);
  print "\nPolling $#apache_mirrors Apache Mirrors";
  print " and Maven Central" unless ($maven_available);
  print "...\n";

  my $start = time();
  $maven_available = (200 == $agent->head($maven_url)->code)
    unless ($maven_available);
  @apache_mirrors = &check_mirrors;
  my $stop = time();
  $sleep_interval = $interval - ($stop - $start);

  my $num_downloadable_apache_mirrors = $num_apache_mirrors - $#apache_mirrors;
  print "$label is ", ($maven_available ? "" : "not "),
  "downloadable from Maven Central.\n" if defined($maven_url);
  printf "$label is downloadable from %d/%d Apache Mirrors (%0.1f%%)\n",
    $num_downloadable_apache_mirrors, $num_apache_mirrors,
    ($num_downloadable_apache_mirrors*100/$num_apache_mirrors);

  last if ($maven_available && 0 == $#apache_mirrors);

  if ($sleep_interval > 0) {
    print "Sleeping for $sleep_interval seconds...\n";
    sleep($sleep_interval)
  }
}

sub check_mirrors {
  my @not_yet_downloadable_apache_mirrors;
  for my $mirror (@apache_mirrors) {

    ### print "\n$mirror\n";
    if (200 != $agent->head($mirror)->code) {
      push @not_yet_downloadable_apache_mirrors, $mirror;
      print $details ? "\nFAIL: $mirror\n" : "X";
    } else {
      print ".";
    }
  }
  print "\n";
  return @not_yet_downloadable_apache_mirrors;
}
