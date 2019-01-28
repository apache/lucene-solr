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
# Spatial search benchmark
#  In order to use this, you'll need to first run 'ant geonames-files'.
#  You may need more memory when running this: -Dtask.mem=1000M
#  For docs on what options are available, see the javadocs.

### Spatial Context, Grid, Strategy config
doc.maker=org.apache.lucene.benchmark.byTask.feeds.SpatialDocMaker
#  SpatialContext:    see SpatialContextFactory.makeSpatialContext
#spatial.spatialContextFactory=org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory
#spatial.geo=true
#spatial.distCalculator=haversine
#spatial.worldBounds=...
#  Spatial Strategy: composite | rpt | ...
spatial.strategy=rpt
#  Spatial Grid: (PrefixTree)  see SpatialPrefixTreeFactory.makeSPT
#spatial.prefixTree=geohash  (or quad)
spatial.prefixTree=quad
#27 quad or 11 geohash are very close
spatial.maxLevels=27
#spatial.maxDistErr (in degrees) to compute maxLevels -- defaults to 1 meter's worth
#  RecursivePrefixTreeStrategy:
#spatial.docPointsOnly=true
#spatial.distErrPct=0.025
#spatial.pruneLeafyBranches=true

#codec.postingsFormat=Memory or (Direct)

### Source & Doc
content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource
line.parser=org.apache.lucene.benchmark.byTask.feeds.GeonamesLineParser
docs.file=work/geonames/allCountries.txt
doc.tokenized=false
#  Next 3 props convert doc points to circles with a random radius and then optionally bbox'es
#doc.spatial.radiusDegrees=0.0
#doc.spatial.radiusDegreesRandPlusMinus=0.0
#doc.spatial.bbox=false

### Directory
directory=FSDirectory
#directory=ByteBuffersDirectory
compound=false
merge.factor=10
ram.flush.mb=64

### Query
query.maker=org.apache.lucene.benchmark.byTask.feeds.SpatialFileQueryMaker
query.file=work/geonames/allCountries.txt
query.file.line.parser=org.apache.lucene.benchmark.byTask.feeds.GeonamesLineParser
query.file.maxQueries=1000
#  Next 3 props convert query points to circles with a random radius and then optionally bbox'es
query.spatial.radiusDegrees=0
query.spatial.radiusDegreesRandPlusMinus=3
query.spatial.bbox=false

#query.spatial.prefixGridScanLevel=scanLevel:-1:-2:-3:-4
#query.spatial.composite.optimizePredicates=optPred:true:false

query.spatial.score=false
#query.spatial.predicate=Intersects
# (defaults to spatial.distErrPct)
#query.spatial.distErrPct=0.0

### Misc

log.step.AddDoc = 100000
task.max.depth.log=1

# -------------------------------------------------------------------------------------

{ "Populate"
  ResetSystemErase
  CreateIndex
  #1 million docs
  [{ "MAddDocs" AddDoc} : 250000] : 4
  ForceMerge(1)
  CommitIndex
  CloseIndex
  
  RepSumByPref MAddDocs
} : 1
#set above round to 0 on subsequent runs if not changing indexing but experimenting with search

OpenReader
{"WarmJIT" Search > : 4000
CloseReader

{ "Rounds"
  ResetSystemSoft
  
  OpenReader
  Search
  {"RealQueries" Search > : 2000
  CloseReader
  
  NewRound
} : 1


#RepSumByName
RepSumByPrefRound RealQueries


