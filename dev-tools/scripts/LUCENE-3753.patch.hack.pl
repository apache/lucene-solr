#!/usr/bin/perl
#
# This script can be used to fix up paths that were moved as a result
# of the structural changes committed as part of LUCENE-3753.
#
# Input is on STDIN, output is to STDOUT
#
# Example use:
#
#    perl LUCENE-3753.patch.hack.pl <my.pre-LUCENE-3753.patch >my.post-LUCENE-3753.patch
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

my @moves = (

    'lucene/src/java'
 => 'lucene/core/src/java',

    'lucene/src/test'
 => 'lucene/core/src/test',

    'lucene/src/resources'
 => 'lucene/core/src/resources',

    'lucene/src/site'
 => 'lucene/site',

    'lucene/src/test-framework/java'
 => 'lucene/test-framework/src/java',

    'lucene/src/test-framework/resources'
 => 'lucene/test-framework/src/resources',

    'lucene/src/tools/java'
 => 'lucene/tools/src/java',

    'lucene/src/tools/javadoc'
 => 'lucene/tools/javadoc',

    'lucene/src/tools/prettify'
 => 'lucene/tools/prettify',

    'dev-tools/maven/lucene/src/pom.xml.template'
 => 'dev-tools/maven/lucene/core/pom.xml.template',
 
    'dev-tools/maven/lucene/src/test-framework/pom.xml.template'
 => 'dev-tools/maven/lucene/test-framework/pom.xml.template',
);

my @copies = ();

my $diff;

while (<>) {
  if (/^Index/) {
    my $next_diff = $_;
    &fixup_paths if ($diff);
    $diff = $next_diff;
  } else {
    $diff .= $_;
  }
}

&fixup_paths; # Handle the final diff

sub fixup_paths {
  for (my $move_pos = 0 ; $move_pos < $#moves ; $move_pos += 2) {
    my $source = $moves[$move_pos];
    my $target = $moves[$move_pos + 1];
    if ($diff =~ /^Index: \Q$source\E/) {
      $diff =~ s/^Index: \Q$source\E/Index: $target/;
      $diff =~ s/\n--- \Q$source\E/\n--- $target/;
      $diff =~ s/\n\+\+\+ \Q$source\E/\n+++ $target/;
      $diff =~ s/\nProperty changes on: \Q$source\E/\nProperty changes on: $target/;
      last;
    }
  }
  print $diff;

  for (my $copy_pos = 0 ; $copy_pos < $#copies ; $copy_pos += 2) {
    my $source = $copies[$copy_pos];
    my $target = $copies[$copy_pos + 1];
    if ($diff =~ /^Index: \Q$source\E/) {
      my $new_diff = $diff;
      $new_diff =~ s/^Index: \Q$source\E/Index: $target/;
      $new_diff =~ s/\n--- \Q$source\E/\n--- $target/;
      $new_diff =~ s/\n\+\+\+ \Q$source\E/\n+++ $target/;
      $new_diff =~ s/\nProperty changes on: \Q$source\E/\nProperty changes on: $target/;
      print $new_diff;
      last;
    }
  }
}
