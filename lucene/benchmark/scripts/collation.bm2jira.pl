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
# ----------
# bm2jira.pl
#
# Converts Lucene contrib-benchmark output produced using the 
# benchmark.collation.alg file into a JIRA-formatted table.
#

use strict;
use warnings;

my %min_elapsed = ();

while (<>) {
  if (/(\S+)(Keyword|JDK|ICU)_\d+\s*([^\s{].*)/) {
    my $lang = $1;
    my $analyzer = $2;
    my $stats = $3;
    my ($elapsed) = $stats =~ /(?:[\d,.]+[-\s]*){4}([.\d]+)/;
    $min_elapsed{$analyzer}{$lang} = $elapsed
      unless (defined($min_elapsed{$analyzer}{$lang})
              && $elapsed >= $min_elapsed{$analyzer}{$lang});
  }
}

# Print out platform info
#print "JAVA:\n", `java -version 2>&1`, "\nOS:\n";
#if ($^O =~ /win/i) {
#  print "$^O\n";
#  eval {
#    require Win32;
#    print Win32::GetOSName(), "\n", Win32::GetOSVersion(), "\n";
#  };
#  die "Error loading Win32: $@" if ($@);
#} else {
#  print `uname -a 2>&1`;
#}

print "\n||Language||java.text||ICU4J||KeywordAnalyzer||ICU4J Improvement||\n";

for my $lang (sort keys %{$min_elapsed{ICU}}) {
  my $ICU = $min_elapsed{ICU}{$lang};
  my $JDK = $min_elapsed{JDK}{$lang};
  my $keyword = $min_elapsed{Keyword}{$lang};
  my $improved = int(100 * ($JDK - $ICU) / ($ICU - $keyword) + 0.5);
  printf "|$lang|${JDK}s|${ICU}s|${keyword}s|\%d%%|\n", $improved;
}
