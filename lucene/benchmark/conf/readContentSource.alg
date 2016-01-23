#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------

#
# This alg reads the information from a ContentSoruce. It is useful for 
# measuring the performance of a particular ContentSource implementation, or 
# gather baselines for operations like indexing (if reading from the content 
# source takes 'X' time, we cannot index faster).
#
# To use this, first cd to benchmark and then run:
#
#   ant run-task -Dtask.alg=conf/readContentSource.alg
#

# Where to get documents from:
content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource
docs.file=temp/enwiki-20070527-pages-articles.xml.bz2

# Stop after processing the document feed once:
content.source.forever=false

# Log messages every:
log.step=100000

# -------------------------------------------------------------------------------------

# Process all documents, appending each one to the line file:
{ ConsumeContentSource } : *

RepSumByPref ConsumeContentSource
