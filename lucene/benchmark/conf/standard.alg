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

writer.version=4.0
merge.factor=mrg:10:100:10:100:10:100:10:100
max.buffered=buf:10:10:100:100:10:10:100:100
compound=cmpnd:true:true:true:true:false:false:false:false

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
#directory=RamDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=2000

docs.dir=reuters-out
#docs.dir=reuters-111

#content.source=org.apache.lucene.benchmark.byTask.feeds.SingleDocSource
content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource

#query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker
query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true
# -------------------------------------------------------------------------------------

{ "Rounds"

    ResetSystemErase

    { "Populate"
        CreateIndex
        { "MAddDocs" AddDoc } : 20000
        ForceMerge(1)
        CloseIndex
    }

    OpenReader  
    { "SearchSameRdr" Search > : 5000
    CloseReader 
                
    { "WarmNewRdr" Warm > : 50
                
    { "SrchNewRdr" Search > : 500
                
    { "SrchTrvNewRdr" SearchTrav > : 300
                
    { "SrchTrvRetNewRdr" SearchTravRet > : 100
                
    OpenReader  
    [ "SearchSameRdr" Search > : 5000 : 2500
    CloseReader 
                
    [ "WarmNewRdr" Warm > : 50 : 25
                
    [ "SrchNewRdr" Search > : 50 : 25
                
    [ "SrchTrvNewRdr" SearchTrav > : 300 : 150
                
    [ "SrchTrvRetNewRdr" SearchTravRet > : 100 : 50

    RepSumByPref MAddDocs

    NewRound

} : 8

RepSumByNameRound
RepSumByName
RepSumByPrefRound MAddDocs
