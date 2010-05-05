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

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
#directory=RamDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=500

docs.dir=reuters-out
#docs.dir=reuters-111

#doc.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleDocMaker
#doc.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersDocMaker
content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource

#query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker
query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker
deletion.policy=org.apache.lucene.index.NoDeletionPolicy

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true
# -------------------------------------------------------------------------------------

{ "Rounds"

    ResetSystemErase

    { "Populate"
        -CreateIndex
        { "MAddDocs" AddDoc > : 1000
        CommitIndex(original)
        CloseIndex
    }

    OpenReader(false,original)
    DeleteByPercent(5)
    { "SearchSameRdr5" Search > : 500
    FlushReader(5%)
    CloseReader 
    PrintReader(5%)

    OpenReader(false,5%)
    DeleteByPercent(10)
    { "SearchSameRdr10" Search > : 500
    FlushReader(10%)
    CloseReader 
    PrintReader(10%)

    OpenReader(false,10%)
    DeleteByPercent(20)
    { "SearchSameRdr20" Search > : 500
    FlushReader(20%)
    CloseReader 
    PrintReader(20%)
    
    OpenReader(false,20%)
    DeleteByPercent(60)
    { "SearchSameRdr60" Search > : 500
    FlushReader(60%)
    CloseReader 
    PrintReader(60%)
    
    OpenReader(false,60%)
    DeleteByPercent(75)
    { "SearchSameRdr75" Search > : 500
    FlushReader(75%)
    CloseReader 
    PrintReader(75%)

    # Test lower percentage of deletes (so undeleteAll is used)
    OpenReader(false,75%)
    DeleteByPercent(7)
    { "SearchSameRdr7" Search > : 500
    FlushReader(7%)
    CloseReader 
    PrintReader(7%)

    NewRound

} : 1

RepSumByName
RepSumByPrefRound MAddDocs
