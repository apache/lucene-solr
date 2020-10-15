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
package org.apache.solr.search.stats;

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Convenience class that wraps a local {@link SolrIndexSearcher} to provide
 * local statistics.
 */
public final class LocalStatsSource extends StatsSource {
  private final StatsCache.StatsCacheMetrics metrics;
  
  public LocalStatsSource(StatsCache.StatsCacheMetrics metrics) {
    this.metrics = metrics;
  }
  
  @Override
  public TermStatistics termStatistics(SolrIndexSearcher localSearcher, Term term, int docFreq, long totalTermFreq)
      throws IOException {
    metrics.missingGlobalTermStats.increment();
    return localSearcher.localTermStatistics(term, docFreq, totalTermFreq);
  }
  
  @Override
  public CollectionStatistics collectionStatistics(SolrIndexSearcher localSearcher, String field)
      throws IOException {
    metrics.missingGlobalFieldStats.increment();
    return localSearcher.localCollectionStatistics(field);
  }
}
