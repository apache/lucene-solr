<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# Apache Solr

Apache Solr is an enterprise search platform written in Java and using [Apache Lucene](https://lucene.apache.org/).
Major features include full-text search, index replication and sharding, and
result faceting and highlighting.


[![Build Status](https://ci-builds.apache.org/job/Solr/job/Solr-Artifacts-main/badge/icon?subject=Solr)](https://ci-builds.apache.org/job/Solr/job/Solr-Artifacts-main/)


## Online Documentation

This README file only contains basic setup instructions.  For more
comprehensive documentation, visit <https://solr.apache.org/guide/>

## Building with Gradle

Firstly, you need to set up your development environment (OpenJDK 11 or greater).

We'll assume that you know how to get and set up the JDK - if you
don't, then we suggest starting at https://www.oracle.com/java/ and learning
more about Java, before returning to this README. Solr runs with
Java 11 and later.

As of 9.0, Solr uses [Gradle](https://gradle.org/) as the build
system. Ant build support has been removed.

To build Solr, run (`./` can be omitted on Windows):

`./gradlew assemble`

NOTE: DO NOT use `gradle` command that is already installed on your machine (unless you know what you'll do).
The "gradle wrapper" (gradlew) does the job - downloads the correct version of it, setups necessary configurations.

The first time you run Gradle, it will create a file "gradle.properties" that
contains machine-specific settings. Normally you can use this file as-is, but it
can be modified if necessary.

The command above packages a full distribution of Solr server; the 
package can be located at:

`solr/packaging/build/solr-*`

Note that the gradle build does not create or copy binaries throughout the
source repository so you need to switch to the packaging output folder above;
the rest of the instructions below remain identical. The packaging directory 
is rewritten on each build. 

For development, especially when you have created test indexes etc, use
the `./gradlew dev` task which will copy binaries to `./solr/packaging/build/dev`
but _only_ overwrite the binaries which will preserve your test setup.

If you want to build the documentation, type `./gradlew -p solr documentation`.

## Running Solr

After building Solr, the server can be started using
the `bin/solr` control scripts.  Solr can be run in either standalone or
distributed (SolrCloud mode).

To run Solr in standalone mode, run the following command from the `solr/`
directory:

`bin/solr start`

To run Solr in SolrCloud mode, run the following command from the `solr/`
directory:

`bin/solr start -c`

The `bin/solr` control script allows heavy modification of the started Solr.
Common options are described in some detail in solr/README.txt.  For an
exhaustive treatment of options, run `bin/solr start -h` from the `solr/`
directory.

### Gradle build and IDE support

- *IntelliJ* - IntelliJ idea can import the project out of the box. 
               Code formatting conventions should be manually adjusted. 
- *Eclipse*  - Not tested.
- *Netbeans* - Not tested.


### Gradle build and tests

`./gradlew assemble` will build a runnable Solr as noted above.

`./gradlew check` will assemble Solr and run all validation
  tasks unit tests.

`./gradlew help` will print a list of help commands for high-level tasks. One
  of these is `helpAnt` that shows the gradle tasks corresponding to ant
  targets you may be familiar with.

## Contributing

Please review the [Contributing to Solr
Guide](https://cwiki.apache.org/confluence/display/solr/HowToContribute) for information on
contributing.

## Discussion and Support

- [Mailing Lists](https://solr.apache.org/community.html#mailing-lists-chat)
- [Issue Tracker (JIRA)](https://issues.apache.org/jira/browse/SOLR)
- IRC: `#solr` and `#solr-dev` on freenode.net
- [Slack](https://solr.apache.org/community.html#slack) 
