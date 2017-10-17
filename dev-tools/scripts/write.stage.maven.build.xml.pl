#!/usr/bin/perl
#
# This script is called from lucene/build.xml and solr/build.xml, by target
# stage-maven-artifacts, to populate an internal Maven repository created by
# generate-maven-artifacts with Ant build files, one per POM.  The
# stage-maven target is then called from each of these Ant build files.
#
# Command line parameters:
#
#  1. The directory in which to find Maven distribution POMs,
#     jars, wars, and signatures.
#  2. The pathname of the Ant build script to be built.
#  3. The pathname of common-build.xml, which will be imported
#     in the Ant build script to be built.
#  4. Whether to prompt for credentials, rather than consulting
#     settings.xml: boolean, e.g. "true" or "false"
#  5. The ID of the target repository
#  6. The URL to the target repository
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
use File::Basename;
use File::Find;
use Cwd 'abs_path';
use File::Path qw(make_path);

my $num_artifacts = 0;
my $maven_dist_dir = abs_path($ARGV[0]);
my $output_build_xml_file = $ARGV[1];
my $common_build_xml = $ARGV[2];
my $m2_credentials_prompt = $ARGV[3];
my $m2_repository_id = $ARGV[4];
if ($^O eq 'cygwin') { # Make sure Cygwin Perl can find the output path
  $output_build_xml_file = `cygpath -u "$output_build_xml_file"`;
  $output_build_xml_file =~ s/\s+$//; # Trim trailing whitespace
  $output_build_xml_file =~ s/^\s+//; # Trim leading whitespace
}
my ($output_file, $output_dir) = fileparse($output_build_xml_file);

my @basepaths = ();
my $grandparent_pom = '';
my @parent_poms = ();
sub find_poms;
File::Find::find({follow => 1, wanted => \&find_poms}, $maven_dist_dir);

my $parent_pom_targets = '';
if (@parent_poms) {
  $parent_pom_targets = "<parent-poms>\n";
  if ($grandparent_pom) {
    $parent_pom_targets .= qq!          <artifact:pom id="grandparent" file="$grandparent_pom"/>\n!;
  }
  my $n = 0;
  for my $parent_pom (@parent_poms) {
    $parent_pom_targets .= qq!          <artifact:pom id="parent.$n" file="$parent_pom"/>\n!;
    ++$n;
  }
  $parent_pom_targets .= "        </parent-poms>\n";
}

make_path($output_dir);
open my $output_build_xml, ">$output_build_xml_file"
    or die "ERROR opening '$ARGV[1]' for writing: $!";

print $output_build_xml qq!<?xml version="1.0"?>
<project xmlns:artifact="antlib:org.apache.maven.artifact.ant">
  <import file="${common_build_xml}"/>

  <target name="stage-maven" depends="install-maven-tasks">
    <sequential>
!;

my $credentials = '';
if ($m2_credentials_prompt !~ /\A(?s:f(?:alse)?|no?)\z/) {
  print $output_build_xml qq!
      <input message="Enter $m2_repository_id username: >" addproperty="m2.repository.username"/>
      <echo>WARNING: ON SOME PLATFORMS YOUR PASSPHRASE WILL BE ECHOED BACK\!\!\!\!\!</echo>
      <input message="Enter $m2_repository_id password: >" addproperty="m2.repository.password">
        <handler type="secure"/>
      </input>\n!;

  $credentials = q!<credentials>
          <authentication username="${m2.repository.username}" password="${m2.repository.password}"/>
        </credentials>!;
}

for my $basepath (@basepaths) {
  output_deploy_stanza($basepath);
}

print $output_build_xml q!
    </sequential>
  </target>
</project>
!;

close $output_build_xml;

print "Wrote '$output_build_xml_file' to stage $num_artifacts Maven artifacts.\n";
exit;

sub find_poms {
  /^(.*)\.pom\z/s && do {
    my $pom_dir = $File::Find::dir;
    if ($^O eq 'cygwin') { # Output windows-style paths on Windows
      $pom_dir = `cygpath -w "$pom_dir"`;
      $pom_dir =~ s/\s+$//; # Trim trailing whitespace
      $pom_dir =~ s/^\s+//; # Trim leading whitespace
    }
    my $basefile = $_;
    $basefile =~ s/\.pom\z//;
    my $basepath = "$pom_dir/$basefile";
    push @basepaths, $basepath;

    if ($basefile =~ /grandparent/) {
      $grandparent_pom = "$basepath.pom";
    } elsif ($basefile =~ /parent/) {
      push @parent_poms, "$basepath.pom";
    }
  }
}

sub output_deploy_stanza {
  my $basepath = shift;
  my $pom_file = "$basepath.pom";
  my $jar_file = "$basepath.jar";
  my $war_file = "$basepath.war";

  if (-f $war_file) {
    print $output_build_xml qq!
      <m2-deploy pom.xml="${pom_file}" jar.file="${war_file}">
        $parent_pom_targets
        <artifact-attachments>
          <attach file="${pom_file}.asc" type="pom.asc"/>
          <attach file="${war_file}.asc" type="war.asc"/>
        </artifact-attachments>
        $credentials
      </m2-deploy>\n!;
  } elsif (-f $jar_file) {
    print $output_build_xml qq!
      <m2-deploy pom.xml="${pom_file}" jar.file="${jar_file}">
        $parent_pom_targets
        <artifact-attachments>
          <attach file="${basepath}-sources.jar" classifier="sources"/>
          <attach file="${basepath}-javadoc.jar" classifier="javadoc"/>
          <attach file="${pom_file}.asc" type="pom.asc"/>
          <attach file="${jar_file}.asc" type="jar.asc"/>
          <attach file="${basepath}-sources.jar.asc" classifier="sources" type="jar.asc"/>
          <attach file="${basepath}-javadoc.jar.asc" classifier="javadoc" type="jar.asc"/>
        </artifact-attachments>
        $credentials
      </m2-deploy>\n!;
  } else {
    print $output_build_xml qq!
      <m2-deploy pom.xml="${pom_file}">
        $parent_pom_targets
        <artifact-attachments>
          <attach file="${pom_file}.asc" type="pom.asc"/>
        </artifact-attachments>
        $credentials
      </m2-deploy>\n!;
  }

  ++$num_artifacts;
}
