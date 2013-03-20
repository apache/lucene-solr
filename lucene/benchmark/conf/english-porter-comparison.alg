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
#
# This alg compares the performance of the original-algorithm English porter
# stemmer (PorterStemmer, via PorterStemFilter) to the Porter2 aka English
# stemmer (EnglishStemmer via SnowballPorterFilter).

content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource
doc.tokenized=false
doc.body.tokenized=true
docs.dir=reuters-out

-AnalyzerFactory(name:original-porter-stemmer,StandardTokenizer,
  StandardFilter,EnglishPossessiveFilter,LowerCaseFilter,StopFilter,
  PorterStemFilter)

-AnalyzerFactory(name:porter2-stemmer,StandardTokenizer,
  StandardFilter,EnglishPossessiveFilter,LowerCaseFilter,StopFilter,
  SnowballPorterFilter(language:English))

-AnalyzerFactory(name:no-stemmer,StandardTokenizer,
  StandardFilter,EnglishPossessiveFilter,LowerCaseFilter,StopFilter)

{ "Rounds"
    -NewAnalyzer(original-porter-stemmer)
    -ResetInputs
    { "Original Porter Stemmer" { ReadTokens > : 20000 }

    -NewAnalyzer(porter2-stemmer)
    -ResetInputs
    { "Porter2/English Stemmer" { ReadTokens > : 20000 }

    -NewAnalyzer(no-stemmer)
    -ResetInputs
    { "No Stemmer" { ReadTokens > : 20000 }

    NewRound
} : 5
RepSumByNameRound
