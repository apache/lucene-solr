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

#DataImportHandler

##Introduction

DataImportHandler is a data import tool for Solr which makes importing data from Databases, XML files and
HTTP data sources quick and easy.

##Important Note

Although Solr strives to be agnostic of the Locale where the server is
running, some code paths in DataImportHandler are known to depend on the
System default Locale, Timezone, or Charset.  It is recommended that when
running Solr you set the following system properties:
  
    -Duser.language=xx -Duser.country=YY -Duser.timezone=ZZZ

where xx, YY, and ZZZ are consistent with any database server's configuration.
