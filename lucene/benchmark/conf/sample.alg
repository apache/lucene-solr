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
# --------------------------------------------------------
# 
# Sample: what is the effect of doc size on indexing time?
#
# There are two parts in this test:
# - PopulateShort adds 2N documents of length  L
# - PopulateLong  adds  N documents of length 2L
# Which one would be faster?
# The comparison is done twice.
#
# --------------------------------------------------------

# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.

merge.factor=mrg:10:20
max.buffered=buf:100:1000
compound=true

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
#directory=ByteBuffersDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=500

docs.dir=reuters-out
#docs.dir=reuters-111

content.source=org.apache.lucene.benchmark.byTask.feeds.SingleDocSource
#content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource

query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker
#query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=false
# -------------------------------------------------------------------------------------

{

    { "PopulateShort"
        CreateIndex
        { AddDoc(4000) > : 20000
        ForceMerge(1)
        CloseIndex
    >

    ResetSystemErase
    
    { "PopulateLong"
        CreateIndex
        { AddDoc(8000) > : 10000
        ForceMerge(1)
        CloseIndex
    >

    ResetSystemErase

    NewRound

} : 2

RepSumByName
RepSelectByPref Populate
