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
# compare.collation.benchmark.jira.tables.pl
#
# Takes as cmdline parameters two JIRA-formatted benchmark results, as produced
# by bm2jira.pl (located in the same directory as this script), and outputs a
# third JIRA-formatted comparison table, showing the differences between two
# benchmarking runs' java.text and ICU4J columns, after accounting for the
# KeywordAnalyzer column; the "ICU4J Improvement" column is ignored.
#
# The difference is calculated as a percentage:
#
#   100 * (patched-rate - unpatched-rate / unpatched-rate)
#
# where the (un)patched-rate is:
#
#   1 / ( elapsed-(un)patched-time - elapsed-KeywordAnalyzer-time)
#

use strict;
use warnings;

my $usage = "Usage: $0 <unpatched-file> <patched-file>\n";

die $usage unless ($#ARGV == 1 && -f $ARGV[0] && -f $ARGV[1]);

my %stats = ();

open UNPATCHED, "<$ARGV[0]" || die "ERROR opening '$ARGV[0]': $!";
while (<UNPATCHED>) {
  # ||Language||java.text||ICU4J||KeywordAnalyzer||ICU4J Improvement||
  # |English|4.51s|2.47s|1.47s|204%|
  next unless (/^\|([^|]+)\|([^|s]+)s\|([^|s]+)s\|([^|s]+)s/);
  my ($lang, $jdk_elapsed, $icu_elapsed, $keyword_analyzer_elapsed)
    = ($1, $2, $3, $4);
  $stats{unpatched}{$lang}{jdk} = $jdk_elapsed;
  $stats{unpatched}{$lang}{icu} = $icu_elapsed;
  $stats{unpatched}{$lang}{keyword_analyzer} = $keyword_analyzer_elapsed;
}
close UNPATCHED;

open PATCHED, "<$ARGV[1]" || die "ERROR opening '$ARGV[1]': $!";
while (<PATCHED>) {
  # ||Language||java.text||ICU4J||KeywordAnalyzer||ICU4J Improvement||
  # |English|4.51s|2.47s|1.47s|204%|
  next unless (/^\|([^|]+)\|([^|s]+)s\|([^|s]+)s\|([^|s]+)s/);
  my ($lang, $jdk_elapsed, $icu_elapsed, $keyword_analyzer_elapsed)
    = ($1, $2, $3, $4);
  $stats{patched}{$lang}{jdk} = $jdk_elapsed;
  $stats{patched}{$lang}{icu} = $icu_elapsed;
  $stats{patched}{$lang}{keyword_analyzer} = $keyword_analyzer_elapsed;
}
close PATCHED;

print "||Language||java.text improvement||ICU4J improvement||\n";
for my $lang (sort keys %{$stats{unpatched}}) {
  my $keyword_analyzer1 = $stats{unpatched}{$lang}{keyword_analyzer};
  my $jdk1 = $stats{unpatched}{$lang}{jdk};
  my $jdk_diff1 = $jdk1 - $keyword_analyzer1;
  my $icu1 = $stats{unpatched}{$lang}{icu};
  my $icu_diff1 = $icu1 - $keyword_analyzer1;

  my $keyword_analyzer2 = $stats{patched}{$lang}{keyword_analyzer};
  my $jdk2 = $stats{patched}{$lang}{jdk};
  my $jdk_diff2 = $jdk2 - $keyword_analyzer2;
  my $icu2 = $stats{patched}{$lang}{icu};
  my $icu_diff2 = $icu2 - $keyword_analyzer2;

  my $jdk_impr 
    = int((1./$jdk_diff2 - 1./$jdk_diff1) / (1./$jdk_diff1) * 1000 + 5) / 10;
  my $icu_impr
    = int((1./$icu_diff2 - 1./$icu_diff1) / (1./$icu_diff1) * 1000 + 5) / 10;

  printf "|$lang|%2.1f%%|%2.1f%%|\n", $jdk_impr, $icu_impr;
}
