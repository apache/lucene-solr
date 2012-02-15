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

ram.flush.mb=flush:32:32
compound=cmpnd:true:false

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=true
doc.term.vector.offsets=true
doc.term.vector.positions=true
log.step=2000

docs.dir=reuters-out

content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource
docs.file=temp/enwiki-20070527-pages-articles.xml

query.maker=org.apache.lucene.benchmark.byTask.feeds.EnwikiQueryMaker
enwikiQueryMaker.disableSpanQueries=true

max.field.length=2147483647
highlighter.maxDocCharsToAnalyze=2147483647

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true
# -------------------------------------------------------------------------------------
{ "Populate"
        CreateIndex
        { "MAddDocs" AddDoc } : 20000
        ForceMerge(1)
        CloseIndex
}
{
        OpenReader
          { "WarmTV" SearchTravRetVectorHighlight(maxFrags[3],fields[body]) > : 100
        CloseReader
}
{
	"Rounds"

        ResetSystemSoft

        OpenReader
          { "SearchVecHlgtSameRdr" SearchTravRetVectorHighlight(maxFrags[3],fields[body]) > : 200
        CloseReader

        ResetSystemSoft

        OpenReader
          { "SearchHlgtSameRdr" SearchTravRetHighlight(maxFrags[3],fields[body]) > : 200
        CloseReader

        RepSumByPref Search

        NewRound
} : 4

RepSumByNameRound
RepSumByName
