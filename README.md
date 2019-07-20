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

# Apache Lucene and Solr

Apache Lucene is a high-performance, full featured text search engine library
written in Java.

Apache Solr is an enterprise search platform written using Apache Lucene.
Major features include full-text search, index replication and sharding, and
result faceting and highlighting.

## Online Documentation

This README file only contains basic setup instructions.  For more
comprehensive documentation, visit:

- Lucene: <http://lucene.apache.org/core/documentation.html>
- Solr: <http://lucene.apache.org/solr/guide/>

## Building Lucene/Solr

(You do not need to do this if you downloaded a pre-built package.)

Lucene and Solr are built using [Apache Ant](http://ant.apache.org/).  To build
Lucene and Solr, run:

`ant compile`

If you see an error about Ivy missing while invoking Ant (e.g., `.ant/lib does
not exist`), run `ant ivy-bootstrap` and retry.

Sometimes you may face issues with Ivy (e.g., an incompletely downloaded artifact).
Cleaning up the Ivy cache and retrying is a workaround for most of such issues: 

`rm -rf ~/.ivy2/cache`

The Solr server can then be packaged and prepared for startup by running the
following command from the `solr/` directory:

`ant server`

## Running Solr

After [building Solr](#building-lucene-solr), the server can be started using
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

## Development/IDEs

Ant can be used to generate project files compatible with most common IDEs.
Run the ant command corresponding to your IDE of choice before attempting to
import Lucene/Solr.

- *Eclipse* - `ant eclipse` (See [this](https://cwiki.apache.org/confluence/display/solr/HowToConfigureEclipse) for details)
- *IntelliJ* - `ant idea` (See [this](https://cwiki.apache.org/confluence/display/lucene/HowtoConfigureIntelliJ) for details)
- *Netbeans* - `ant netbeans` (See [this](https://cwiki.apache.org/confluence/display/lucene/HowtoConfigureNetbeans) for details)


## Running Tests

The standard test suite can be run with the command:

`ant test`

Like Solr itself, the test-running can be customized or tailored in a number or
ways.  For an exhaustive discussion of the options available, run:

`ant test-help`

## Contributing

Please review the [Contributing to Solr
Guide](https://cwiki.apache.org/confluence/display/solr/HowToContribute) for information on
contributing.

## Discussion and Support

- [Users Mailing List](http://lucene.apache.org/solr/community.html#solr-user-list-solr-userluceneapacheorg)
- [Developers Mailing List](http://lucene.apache.org/solr/community.html#developer-list-devluceneapacheorg)
- [Lucene Issue Tracker](https://issues.apache.org/jira/browse/LUCENE)
- [Solr Issue Tracker](https://issues.apache.org/jira/browse/SOLR)
- IRC: `#solr` and `#solr-dev` on freenode.net
