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
# multi val params are iterated by NewRound's, added to reports, start with column name.
#
# based on micro-standard
#
# modified to use wikipedia sources and index entire docs
# currently just used to measure ingest rate

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory

work.dir = /x/lucene/wiki.5M

doc.stored=true
doc.body.stored=false
doc.tokenized=false
doc.body.tokenized=true
doc.term.vector=false
log.step.AddDoc = 10000
log.step.Search = 10000
compound = false

content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource
content.source.forever = false
file.query.maker.file = queries.txt

query.maker=org.apache.lucene.benchmark.byTask.feeds.FileBasedQueryMaker
docs.file = /x/lucene/enwiki-20090306-lines-1k-fixed.txt

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true

# -------------------------------------------------------------------------------------

# Open a writer
OpenIndex
{
  # Get a new near-real-time reader, once per second:
  NearRealtimeReader(1.0) &

  # Warm
  Search

  # Index with 2 threads, each adding 100 docs per sec
  [ "Indexing" { AddDoc > : * : 100/sec ] : 2 &

  # Redline search (from queries.txt) with 4 threads
  [ "Searching" { Search > : * ] : 4 &

  # Wait 60 sec, then wrap up
  Wait(5.0)
}
CloseReader

# Don't keep any changes, so we can re-test on the same index again
RollbackIndex

RepSumByPref Indexing
RepSumByPref Searching
RepSumByPref NearRealtimeReader


