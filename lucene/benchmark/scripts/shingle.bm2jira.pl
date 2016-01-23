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
# shingle.bm2jira.pl
#
# Converts Lucene contrib-benchmark output produced using the 
# conf/shingle.alg file into a JIRA-formatted table.
#

use strict;
use warnings;

my %min_elapsed = ();

#Operation           round  runCnt  recsPerRun  rec/s      elapsedSec  avgUsedMem  avgTotalMem
#BigramsAndUnigrams  0      1       255691      21,147.22  12.09       15,501,840  35,061,760
#BigramsOnly   -  -  0 -  - 1 -  -  127383   -  16,871.92  7.55    -   31,725,312  41,746,432
#FourgramsAndUnigrams
#FourgramsOnly
#UnigramsOnly

while (<>) {
  if (/^((?:Uni|Bi|Four)grams\S+)[-\s]*([^\s{].*)/) {
    my $operation = $1;
    my $stats = $2;
    my $max_shingle_size 
    = ($operation =~ /^Bigrams/ ? 2 : $operation =~ /^Unigrams/ ? 1 : 4);
    my $output_unigrams 
      = ($operation =~ /(?:AndUnigrams|UnigramsOnly)$/ ? 'yes' : 'no'); 
    my ($elapsed) = $stats =~ /(?:[\d,.]+[-\s]*){4}([.\d]+)/;
    $min_elapsed{$max_shingle_size}{$output_unigrams} = $elapsed
      unless (defined($min_elapsed{$max_shingle_size}{$output_unigrams})
              && $elapsed >= $min_elapsed{$max_shingle_size}{$output_unigrams});
  }
}

# Print out platform info
print "JAVA:\n", `java -version 2>&1`, "\nOS:\n";
if ($^O =~ /(?<!dar)win/i) {
  print "$^O\n";
  eval {
    require Win32;
    print Win32::GetOSName(), "\n", Win32::GetOSVersion(), "\n";
  };
  die "Error loading Win32: $@" if ($@);
} else {
  print `uname -a 2>&1`;
}

print "\n||Max Shingle Size||Unigrams?||Elapsed||\n";

for my $max_shingle_size (sort { $a <=> $b } keys %min_elapsed) {
  for my $output_unigrams (sort keys %{$min_elapsed{$max_shingle_size}}) {
    my $size = (1 == $max_shingle_size ? '1 (Unigrams)' : $max_shingle_size);   
    printf "|$size|$output_unigrams|\%2.2fs|\n",
           $min_elapsed{$max_shingle_size}{$output_unigrams};
  }
}
