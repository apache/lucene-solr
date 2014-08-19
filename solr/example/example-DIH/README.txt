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

Solr DataImportHandler example configuration
--------------------------------------------

Change to the parent (example) directory. Start solr by executing the following command 

> cd ..
> java -Dsolr.solr.home="./example-DIH/solr/" -jar start.jar

in this directory, and when Solr is started connect to:

  http://localhost:8983/solr/

* To import data from the hsqldb database, connect to:

  http://localhost:8983/solr/db/dataimport?command=full-import

* To import data from an RSS feed, connect to:

  http://localhost:8983/solr/rss/dataimport?command=full-import

* To import data from your IMAP server:

  1. Edit the example-DIH/solr/mail/conf/mail-data-config.xml and add details about username, password, IMAP server
  2. Connect to http://localhost:8983/solr/mail/dataimport?command=full-import

* To copy data from db Solr core, connect to:

  http://localhost:8983/solr/solr/dataimport?command=full-import

* To index a full text document using Tika integration:

  http://localhost:8983/solr/tika/dataimport?command=full-import

See also README.txt in the solr subdirectory, and check
https://cwiki.apache.org/confluence/display/solr/Uploading+Structured+Data+Store+Data+with+the+Data+Import+Handler
for detailed usage guide and tutorial.
