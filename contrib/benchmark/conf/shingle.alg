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

content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource
doc.tokenized=false
doc.body.tokenized=true
docs.dir=reuters-out
log.step=1000

{ "Rounds"

    -NewShingleAnalyzer(maxShingleSize:2,outputUnigrams:true)
    -ResetInputs
    { "BigramsAndUnigrams" { ReadTokens > : 10000 }

    -NewShingleAnalyzer(maxShingleSize:2,outputUnigrams:false)
    -ResetInputs
    { "BigramsOnly" { ReadTokens > : 10000 }

    -NewShingleAnalyzer(maxShingleSize:4,outputUnigrams:true)
    -ResetInputs
    { "FourgramsAndUnigrams" { ReadTokens > : 10000 }

    -NewShingleAnalyzer(maxShingleSize:4,outputUnigrams:false)
    -ResetInputs
    { "FourgramsOnly" { ReadTokens > : 10000 }

    -NewAnalyzer(standard.StandardAnalyzer)
    -ResetInputs
    { "UnigramsOnly" { ReadTokens > : 10000 }

    NewRound

} : 5

RepSumByNameRound
