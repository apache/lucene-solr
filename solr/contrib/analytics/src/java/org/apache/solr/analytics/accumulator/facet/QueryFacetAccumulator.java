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
import org.apache.solr.analytics.accumulator.ValueAccumulator;
import org.apache.solr.analytics.statistics.StatsCollector;
import org.apache.solr.common.util.NamedList;

/**
 * An Accumulator that manages a certain query of a given query facet.
 */
public class QueryFacetAccumulator extends ValueAccumulator {
  protected final FacetValueAccumulator parent;
  protected final String facetName;
  protected final String facetValue;

  public QueryFacetAccumulator(FacetValueAccumulator parent, String facetName, String facetValue) {
    this.parent = parent;
    this.facetName = facetName;
    this.facetValue = facetValue;
  }

  /**
   * Tell the FacetingAccumulator to collect the doc with the 
   * given queryFacet and query.
   */
  @Override
  public void collect(int doc) throws IOException {
    parent.collectQuery(doc, facetName, facetValue);
  }

  /**
   * Update the readers of the queryFacet {@link StatsCollector}s in FacetingAccumulator
   */
  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    parent.setQueryStatsCollectorReaders(context);
  }

  @Override
  public void compute() {
    // NOP
  }

  @Override
  public NamedList<?> export() {
    // NOP
    return null;
  }

}
