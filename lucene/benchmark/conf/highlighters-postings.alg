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

# For postings-offsets with light term-vectors

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
work.dir=work/enwikiPostings
ram.flush.mb=64
compound=false

doc.stored=true
doc.tokenized=true
# offsets in postings:
doc.body.offsets=true
# term vector, but no positions/offsets with it
doc.term.vector=true

content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource
docs.file=temp/enwiki-20070527-pages-articles.xml.bz2

query.maker=org.apache.lucene.benchmark.byTask.feeds.FileBasedQueryMaker
file.query.maker.file=conf/query-terms.txt
log.queries=false
log.step.SearchTravRetHighlight=-1

highlighter=HlImpl:NONE:SH_A:UH_A:UH_P:UH_PV

{ "Populate"
        CreateIndex
        [{ "MAddDocs" AddDoc > : 50000] : 4
        CloseIndex
    } : 0

{
	"Rounds"

        ResetSystemSoft

        OpenReader

        { "Warm" SearchTravRetHighlight > : 1000

        { "HL" SearchTravRetHighlight > : 2000

        CloseReader

        NewRound
} : 5

RepSumByPrefRound HL