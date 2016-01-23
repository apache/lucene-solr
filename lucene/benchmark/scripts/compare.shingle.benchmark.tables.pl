#!/usr/bin/perl
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
# ------------------------------------------
# compare.shingle.benchmark.jira.tables.pl
#
# Takes as cmdline parameters two JIRA-formatted benchmark results, as produced
# by shingle.bm2jira.pl (located in the same directory as this script), and
# outputs a third JIRA-formatted comparison table.
#
# The difference is calculated as a percentage:
#
#   100 * (unpatched-elapsed - patched-elapsed / patched-elapsed)
#
# where (un)patched-elapsed values have had the no-shingle-filter 
# (StandardAnalyzer) elapsed time subtracted from them.
#
#
# Example shingle.bm2jira.pl output:
# ----------------------------------
# JAVA:
# java version "1.5.0_15"
# Java(TM) 2 Runtime Environment, Standard Edition (build 1.5.0_15-b04)
# Java HotSpot(TM) 64-Bit Server VM (build 1.5.0_15-b04, mixed mode)
#
# OS:
# cygwin
# WinVistaService Pack 2
# Service Pack 26060022202561
#
# ||Max Shingle Size||Unigrams?||Elapsed||
# |1 (Unigrams)|yes|2.19s|
# |2|no|4.74s|
# |2|yes|4.90s|
# |4|no|5.82s|
# |4|yes|5.97s|

use strict;
use warnings;

my $usage = "Usage: $0 <unpatched-file> <patched-file>\n";

die $usage unless ($#ARGV == 1 && -f $ARGV[0] && -f $ARGV[1]);

my %stats = ();

open UNPATCHED, "<$ARGV[0]" || die "ERROR opening '$ARGV[0]': $!";
my $table_encountered = 0;
my $standard_analyzer_elapsed = 0;
my %unpatched_stats = ();
my %patched_stats = ();
while (<UNPATCHED>) {
  unless ($table_encountered) {
    if (/\Q||Max Shingle Size||Unigrams?||Elapsed||\E/) {
      $table_encountered = 1;
    } else {
      print;
    }
  } elsif (/\|([^|]+)\|([^|]+)\|([\d.]+)s\|/) {
    my $max_shingle_size = $1;
    my $output_unigrams = $2;
    my $elapsed = $3;
    if ($max_shingle_size =~ /Unigrams/) {
      $standard_analyzer_elapsed = $elapsed;
    } else {
      $unpatched_stats{$max_shingle_size}{$output_unigrams} = $elapsed;
    }
  }
}
close UNPATCHED;

open PATCHED, "<$ARGV[1]" || die "ERROR opening '$ARGV[1]': $!";
while (<PATCHED>) {
  if (/\|([^|]+)\|([^|]+)\|([\d.]+)s\|/) {
    my $max_shingle_size = $1;
    my $output_unigrams = $2;
    my $elapsed = $3;
    if ($max_shingle_size =~ /Unigrams/) {
      $standard_analyzer_elapsed = $elapsed
         if ($elapsed < $standard_analyzer_elapsed);
    } else {
      $patched_stats{$max_shingle_size}{$output_unigrams} = $elapsed;
    }
  }
}
close PATCHED;

print "||Max Shingle Size||Unigrams?||Unpatched||Patched||StandardAnalyzer||Improvement||\n";
for my $max_shingle_size (sort { $a <=> $b } keys %unpatched_stats) {
  for my $output_unigrams (sort keys %{$unpatched_stats{$max_shingle_size}}) {
    my $improvement 
      = ( $unpatched_stats{$max_shingle_size}{$output_unigrams}
        - $patched_stats{$max_shingle_size}{$output_unigrams})
      / ( $patched_stats{$max_shingle_size}{$output_unigrams}
        - $standard_analyzer_elapsed);
    $improvement = int($improvement * 1000 + .5) / 10; # Round and truncate
    printf "|$max_shingle_size|$output_unigrams"
          ."|$unpatched_stats{$max_shingle_size}{$output_unigrams}s"
          ."|$patched_stats{$max_shingle_size}{$output_unigrams}s"
          ."|${standard_analyzer_elapsed}s|%2.1f%%|\n", $improvement;
  }
}
