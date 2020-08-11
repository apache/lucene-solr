#!/usr/bin/perl
#
# This script can be used to fix up paths that were moved as a result
# of the structural changes committed as part of SOLR-2452.
#
# Input is on STDIN, output is to STDOUT
#
# Example use:
#
#    perl SOLR-2452.patch.hack.pl <my.pre-SOLR-2452.patch >my.post-SOLR-2452.patch
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
    'solr/contrib/analysis-extras/src/test-files/solr-analysis-extras'
 => 'solr/contrib/analysis-extras/src/test-files/analysis-extras/solr',

    'solr/contrib/analysis-extras/src/test-files'
 => 'solr/contrib/analysis-extras/src/test-files/analysis-extras',

    'solr/contrib/clustering/src/test/java'
 => 'solr/contrib/clustering/src/test',

    'solr/contrib/clustering/src/test/resources/solr-clustering'
 => 'solr/contrib/clustering/src/test-files/clustering/solr',

    'solr/contrib/clustering/src/test/resources'
 => 'solr/contrib/clustering/src/test-files/clustering',

    'solr/contrib/clustering/src/main/java'
 => 'solr/contrib/clustering/src/java',

    'solr/contrib/dataimporthandler/src/test/java'
 => 'solr/contrib/dataimporthandler/src/test',

    'solr/contrib/dataimporthandler/src/test/resources/solr-dih'
 => 'solr/contrib/dataimporthandler/src/test-files/dih/solr',

    'solr/contrib/dataimporthandler/src/test/resources'
 => 'solr/contrib/dataimporthandler/src/test-files/dih',

    'solr/contrib/dataimporthandler/src/main/java'
 => 'solr/contrib/dataimporthandler/src/java',

    'solr/contrib/dataimporthandler/src/main/webapp'
 => 'solr/contrib/dataimporthandler/src/webapp',

    'solr/contrib/dataimporthandler/src/extras/test/java'
 => 'solr/contrib/dataimporthandler-extras/src/test',

    'solr/contrib/dataimporthandler/src/extras/test/resources/solr-dihextras'
 => 'solr/contrib/dataimporthandler-extras/src/test-files/dihextras/solr',

    'solr/contrib/dataimporthandler/src/extras/test/resources'
 => 'solr/contrib/dataimporthandler-extras/src/test-files/dihextras',

    'solr/contrib/dataimporthandler/src/extras/main/java'
 => 'solr/contrib/dataimporthandler-extras/src/java',

    'solr/contrib/extraction/src/test/java'
 => 'solr/contrib/extraction/src/test',

    'solr/contrib/extraction/src/test/resources/solr-extraction'
 => 'solr/contrib/extraction/src/test-files/extraction/solr',

    'solr/contrib/extraction/src/test/resources'
 => 'solr/contrib/extraction/src/test-files/extraction',

    'solr/contrib/extraction/src/main/java'
 => 'solr/contrib/extraction/src/java',

    'solr/src/test-files/books.csv'
 => 'solr/solrj/src/test-files/solrj/books.csv',

    'solr/src/test-files/sampleDateFacetResponse.xml'
 => 'solr/solrj/src/test-files/solrj/sampleDateFacetResponse.xml',

    'solr/src/test-files/solr/shared'
 => 'solr/solrj/src/test-files/solrj/solr/shared',

    'solr/src/solrj'
 => 'solr/solrj/src/java',

    'solr/src/common'
 => 'solr/solrj/src/java',

    'solr/src/test/org/apache/solr/common'
 => 'solr/solrj/src/test/org/apache/solr/common',

    'solr/src/test/org/apache/solr/client/solrj/SolrJettyTestBase.java'
 => 'solr/test-framework/src/java/org/apache/solr/SolrJettyTestBase.java',

    'solr/src/test/org/apache/solr/client/solrj'
 => 'solr/solrj/src/test/org/apache/solr/client/solrj',

    'solr/src/test-framework'
 => 'solr/test-framework/src/java',

    'solr/src/test/org/apache/solr/util/ExternalPaths.java'
 => 'solr/test-framework/src/java/org/apache/solr/util/ExternalPaths.java',

    'solr/src/java'
 => 'solr/core/src/java',

    'solr/src/test'
 => 'solr/core/src/test',

    'solr/src/test-files'
 => 'solr/core/src/test-files',

    'solr/src/webapp/src'
 => 'solr/core/src/java',

    'solr/src/webapp/web'
 => 'solr/webapp/web',

    'solr/src/scripts'
 => 'solr/scripts',

    'solr/src/dev-tools'
 => 'solr/dev-tools',

    'solr/src/site'
 => 'solr/site-src',

    'dev-tools/maven/solr/src/pom.xml.template'
 => 'dev-tools/maven/solr/core/pom.xml.template',

    'dev-tools/maven/solr/src/test-framework/pom.xml.template'
 => 'dev-tools/maven/solr/test-framework/pom.xml.template',

    'dev-tools/maven/solr/src/solrj/pom.xml.template'
 => 'dev-tools/maven/solr/solrj/pom.xml.template',

    'dev-tools/maven/solr/src/webapp/pom.xml.template'
 => 'dev-tools/maven/solr/webapp/pom.xml.template',
);

my @copies = (
    'solr/core/src/test-files/README'
 => 'solr/solrj/src/test-files/solrj/README',

    'solr/core/src/test-files/solr/crazy-path-to-schema.xml'
 => 'solr/solrj/src/test-files/solrj/solr/crazy-path-to-schema.xml',

    'solr/core/src/test-files/solr/conf/schema.xml'
 => 'solr/solrj/src/test-files/solrj/solr/conf/schema.xml',

    'solr/core/src/test-files/solr/conf/schema-replication1.xml'
 => 'solr/solrj/src/test-files/solrj/solr/conf/schema-replication1.xml',

    'solr/core/src/test-files/solr/conf/solrconfig-follower1.xml'
 => 'solr/solrj/src/test-files/solrj/solr/conf/solrconfig-follower1.xml',
);

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
