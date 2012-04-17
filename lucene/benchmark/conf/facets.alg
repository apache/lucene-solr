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

with.facets=facets:true:false

content.source.forever=false

compound=true
analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
taxonomy.directory=FSDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=1000

docs.dir=reuters-out

content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource

facet.source=org.apache.lucene.benchmark.byTask.feeds.RandomFacetSource
rand.seed=10
max.doc.facets=20
max.facet.depth=3

query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

task.max.depth.log=2

#log.queries=true
# -------------------------------------------------------------------------------------

{ "Rounds"
  ResetSystemErase
  { "Populate"
      -CreateIndex
      -CreateTaxonomyIndex
      { "MAddDocs" AddFacetedDoc > : *
      -ForceMerge(1)
      -CloseIndex
      -CloseTaxonomyIndex
  }

  OpenReader  
  { "SearchSameRdr" Search > : 40
  CloseReader 

  #RepSumByNameRound
  ResetSystemErase
  NewRound
} : 4

RepSumByPrefRound Search
RepSumByPrefRound Populate
RepSumByPrefRound MAddDocs

