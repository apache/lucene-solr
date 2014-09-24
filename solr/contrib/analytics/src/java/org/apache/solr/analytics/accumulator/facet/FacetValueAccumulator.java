/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.analytics.accumulator.facet;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;

/**
 * Interface that describes the methods needed for an Accumulator to be able to handle 
 * fieldFacets, rangeFacets and queryFacets.
 */
public interface FacetValueAccumulator {

  void collectField(int doc, String facetName, String facetValue) throws IOException;
  void collectQuery(int doc, String facetName, String facetValue) throws IOException;
  void collectRange(int doc, String facetName, String facetValue) throws IOException;
  void setQueryStatsCollectorReaders(LeafReaderContext context) throws IOException;
  void setRangeStatsCollectorReaders(LeafReaderContext context) throws IOException;

}
