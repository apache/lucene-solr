# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource
content.source.encoding=UTF-8
doc.tokenized=false
doc.body.tokenized=true
docs.file=work/top100k-out/top.fr.wikipedia.words.txt
content.source.forever=false
log.step=100000

{ "Rounds"
    -NewAnalyzer(KeywordAnalyzer)
    -SetProp(docs.file,work/top100k-out/top.fr.wikipedia.words.txt)
    ResetInputs
    { "FrenchKeyword" { ReadTokens > : * ResetInputs } : 10

    -NewAnalyzer(KeywordAnalyzer)
    -SetProp(docs.file,work/top100k-out/top.de.wikipedia.words.txt)
    ResetInputs
    { "GermanKeyword" { ReadTokens > : * ResetInputs } : 10

    -NewAnalyzer(KeywordAnalyzer)
    -SetProp(docs.file,work/top100k-out/top.uk.wikipedia.words.txt)
    ResetInputs
    { "UkrainianKeyword" { ReadTokens > : * ResetInputs } : 10
 
    -NewAnalyzer(KeywordAnalyzer)
    -SetProp(docs.file,work/top100k-out/top.en.wikipedia.words.txt)
    ResetInputs
    { "EnglishKeyword" { ReadTokens > : * ResetInputs } : 10
 
    -NewLocale(fr)
    -NewCollationAnalyzer
    -SetProp(docs.file,work/top100k-out/top.fr.wikipedia.words.txt)
    ResetInputs
    { "FrenchJDK" { ReadTokens > : * ResetInputs } : 10

    -NewLocale(de)
    -NewCollationAnalyzer
    -SetProp(docs.file,work/top100k-out/top.de.wikipedia.words.txt)
    ResetInputs
    { "GermanJDK" { ReadTokens > : * ResetInputs } : 10

    -NewLocale(uk)
    -NewCollationAnalyzer
    -SetProp(docs.file,work/top100k-out/top.uk.wikipedia.words.txt)
    ResetInputs
    { "UkrainianJDK" { ReadTokens > : * ResetInputs } : 10

    -NewLocale(en)
    -NewCollationAnalyzer
    -SetProp(docs.file,work/top100k-out/top.en.wikipedia.words.txt)
    ResetInputs
    { "EnglishJDK" { ReadTokens > : * ResetInputs } : 10

    -NewLocale(fr)
    -NewCollationAnalyzer(impl:icu)
    -SetProp(docs.file,work/top100k-out/top.fr.wikipedia.words.txt)
    ResetInputs
    { "FrenchICU" { ReadTokens > : * ResetInputs } : 10

    -NewLocale(de)
    -NewCollationAnalyzer(impl:icu)
    -SetProp(docs.file,work/top100k-out/top.de.wikipedia.words.txt)
    ResetInputs
    { "GermanICU" { ReadTokens > : * ResetInputs } : 10

    -NewLocale(uk)
    -NewCollationAnalyzer(impl:icu)
    -SetProp(docs.file,work/top100k-out/top.uk.wikipedia.words.txt)
    ResetInputs
    { "UkrainianICU" { ReadTokens > : * ResetInputs } : 10

    -NewLocale(en)
    -NewCollationAnalyzer(impl:icu)
    -SetProp(docs.file,work/top100k-out/top.en.wikipedia.words.txt)
    ResetInputs
    { "EnglishICU" { ReadTokens > : * ResetInputs } : 10

    NewRound

} : 5

RepSumByNameRound
