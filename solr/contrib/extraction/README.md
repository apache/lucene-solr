<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Apache Solr Content Extraction Library (Solr Cell)

##Introduction

Apache Solr Extraction provides a means for extracting and indexing content contained in "rich" documents, such
as Microsoft Word, Adobe PDF, etc.  (Each name is a trademark of their respective owners)  This contrib module
uses Apache Tika to extract content and metadata from the files, which can then be indexed.  For more information,
see http://wiki.apache.org/solr/ExtractingRequestHandler

##Getting Started

You will need Solr up and running.  Then, simply add the extraction JAR file, plus the Tika dependencies (in the ./lib folder)
to your Solr Home lib directory.  See http://wiki.apache.org/solr/ExtractingRequestHandler for more details on hooking it in
 and configuring.

