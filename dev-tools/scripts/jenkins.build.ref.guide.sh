#!/usr/bin/env bash

# This shell script will download the software required to build the ref
# guide using RVM (Ruby Version Manager), and then run the following
# under solr/solr-ref-guide: "ant clean build-site build-pdf".
#
# The following will be downloaded and installed into $HOME/.rvm/:
# RVM, Ruby, and Ruby gems jekyll, asciidoctor, jekyll-asciidoc,
# and pygments.rb.
#
# The script expects to be run in the top-level project directory.
#
# RVM will attempt to verify the signature on downloaded RVM software if
# you have gpg or gpg2 installed.  If you do, as a one-time operation you
# must import two keys (substitute gpg2 below if you have it installed):
#
#    gpg --keyserver hkp://keys.gnupg.net --recv-keys \
#        409B6B1796C275462A1703113804BB82D39DC0E3     \
#        7D2BAF1CF37B13E2069D6956105BD0E739499BDB
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

set -x                                   # Echo commands to the console
set -e                                   # Fail the script if any command fails

RVM_PATH=$HOME/.rvm
RUBY_VERSION=ruby-2.5.1
GEMSET=solr-refguide-gemset

# Install the "stable" RVM release to ~/.rvm/, and don't mess with .bash_profile etc.
\curl -sSL https://get.rvm.io | bash -s -- --ignore-dotfiles stable

set +x                                   # Temporarily disable command echoing to reduce clutter

function echoRun() {
    local cmd="$1"
    echo "Running '$cmd'"
    $cmd
}

echoRun "source $RVM_PATH/scripts/rvm"   # Load RVM into a shell session *as a Bash function*
echoRun "rvm cleanup all"                # Remove old stuff
echoRun "rvm autolibs disable"           # Enable single-user mode
echoRun "rvm install $RUBY_VERSION"      # Install Ruby
echoRun "rvm gemset create $GEMSET"      # Create this project's gemset
echoRun "rvm $RUBY_VERSION@$GEMSET"      # Activate this project's gemset

# Install gems in the gemset.  Param --force disables dependency conflict detection.
echoRun "gem install --force --version 3.5.0 jekyll"
echoRun "gem uninstall --all --ignore-dependencies asciidoctor"  # Get rid of all versions
echoRun "gem install --force --version 2.0.10 asciidoctor"
echoRun "gem install --force --version 3.0.0 jekyll-asciidoc"
echoRun "gem install --force --version 4.0.1 slim"
echoRun "gem install --force --version 2.0.10 tilt"
echoRun "gem install --force --version 1.1.5 concurrent-ruby"

cd solr/solr-ref-guide

set -x                                   # Re-enable command echoing
ant clean build-site
