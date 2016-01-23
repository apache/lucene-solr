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
# Compound: what is the cost of compound format in indexing?
# It does twice as much IO, is it twice slower? (no)
# --------------------------------------------------------

# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.

merge.factor=mrg:10
max.buffered=buf:10
compound=compnd:true:false

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
#directory=RamDirectory

doc.stored=stored:true:true:false:false
doc.tokenized=true
doc.term.vector=vector:true:true:false:false
log.step=500
log.step.DeleteDoc=100

docs.dir=reuters-out
#docs.dir=reuters-111

content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource

#query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker
query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=1

log.queries=false
# -------------------------------------------------------------------------------------

ResetSystemErase

{ "Round"
  CreateIndex
  { "AddDocs" AddDoc > : 10000
  CloseIndex

  OpenReader  
  { "SearchSameRdr" Search > : 500
  CloseReader 
              
  { "WarmNewRdr" Warm > : 50
              
  { "SrchNewRdr" Search > : 500
              
  { "SrchTrvNewRdr" SearchTrav > : 300
              
  { "SrchTrvRetNewRdr" SearchTravRet > : 100

  [ "WarmNewRdr" Warm > : 50
              
  [ "SrchNewRdr" Search > : 500
              
  [ "SrchTrvNewRdr" SearchTrav > : 300
              
  [ "SrchTrvRetNewRdr" SearchTravRet > : 100

  ResetInputs
  RepSumByName
  NewRound
} : 4
            
RepSumByName
RepSumByNameRound
RepSumByPrefRound AddDocs
RepSumByPrefRound SearchSameRdr
RepSumByPrefRound WarmNewRdr
RepSumByPrefRound SrchTrvNewRdr
RepSumByPrefRound SrchTrvRetNewRdr
