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
# This file indexes documents contained in a single text file, one per
# line.  See createLineFile.alg for how to create this file.  The
# benefit of this is it removes the IO cost of opening one file per
# document to let you more accurately measure time spent analyzing and
# indexing your documents vs time spent creating the documents.
#
# To use this, you must first run the createLineFile.alg, then cd to
# benchmark and then run:
#
#   ant run-task -Dtask.alg=conf/indexLineFile.alg
#

analyzer=org.apache.lucene.analysis.core.SimpleAnalyzer

# Feed that knows how to process the line file format:
content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource

# File that contains one document per line:
docs.file=work/reuters.lines.txt

# Process documents only once:
content.source.forever=false

# -------------------------------------------------------------------------------------

# Reset the system, create a new index, index all docs from the line
# file, close the index, produce a report.

ResetSystemErase
CreateIndex
{AddDoc}: *
CloseIndex

RepSumByPref AddDoc 
