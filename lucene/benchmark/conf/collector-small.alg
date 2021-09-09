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

# collector.class can be:
#    Fully Qualified Class Name of a Collector with a empty constructor
#    topScoreDocOrdered - Creates a TopScoreDocCollector that requires in order docs
#    topScoreDocUnordered - Like above, but allows out of order
collector.class=coll:topScoreDoc

analyzer=org.apache.lucene.analysis.core.WhitespaceAnalyzer
directory=FSDirectory
#directory=RamDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=100000

search.num.hits=100000

content.source=org.apache.lucene.benchmark.byTask.feeds.LongToEnglishContentSource


query.maker=org.apache.lucene.benchmark.byTask.feeds.LongToEnglishQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true
# -------------------------------------------------------------------------------------

{ "Rounds"

    ResetSystemErase

    { "Populate"
        CreateIndex
        { "MAddDocs" AddDoc } : 200000
        ForceMerge(1)
        CloseIndex
    }

    OpenReader
    { "topDocs" SearchWithCollector > : 10
    CloseReader

#    OpenReader
#uses an array of search.num.hits size, but can also take in a parameter
#    { "psc" SearchWithPostSortCollector > : 10
#    { "psc100" SearchWithPostSortCollector(100) > : 10
#    { "psc1000" SearchWithPostSortCollector(1000) > : 10
#    { "psc10000" SearchWithPostSortCollector(10000) > : 10
#    { "psc50000" SearchWithPostSortCollector(50000) > : 10
#    CloseReader

    RepSumByPref topDocs
#    RepSumByPref psc
#    RepSumByPref psc100
#    RepSumByPref psc1000
#    RepSumByPref psc10000
#    RepSumByPref psc50000

    NewRound

} : 4

#RepSumByNameRound
#RepSumByName
#RepSumByPrefRound topDocs
#RepSumByPrefRound psc
#RepSumByPrefRound psc100
#RepSumByPrefRound psc1000
#RepSumByPrefRound psc10000
#RepSumByPrefRound psc50000

