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

writer.version=4.0

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
work.dir=output

# directory to add to the target index
addindexes.input.dir=input/index

# task at this depth or less would print when they start
task.max.depth.log=2

# -------------------------------------------------------------------------------------

# call addIndexes (Directory)
ResetSystemErase
CreateIndex
{ "AddIndexesDirectory" AddIndexes(true) >
CloseIndex

# call addIndexes (IndexReader)
ResetSystemErase
CreateIndex
{ "AddIndexesReader" AddIndexes(false) >
CloseIndex

RepSumByName
RepSumByPref AddIndexes