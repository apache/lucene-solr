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

import org.apache.lucene.search.CollectionStatistics;

/**
 * Modifiable version of {@link CollectionStatistics} useful for
 * aggregation of per-shard stats.
 */
public class CollectionStats {
  public final String field;
  public long maxDoc;
  public long docCount;
  public long sumTotalTermFreq;
  public long sumDocFreq;
  
  public CollectionStats(String field) {
    this.field = field;
  }
  
  public CollectionStats(String field, long maxDoc, long docCount,
          long sumTotalTermFreq, long sumDocFreq) {
    this.field = field;
    this.maxDoc = maxDoc;
    this.docCount = docCount;
    this.sumTotalTermFreq = sumTotalTermFreq;
    this.sumDocFreq = sumDocFreq;
  }
  
  public CollectionStats(CollectionStatistics stats) {
    this.field = stats.field();
    this.maxDoc = stats.maxDoc();
    this.docCount = stats.docCount();
    this.sumTotalTermFreq = stats.sumTotalTermFreq();
    this.sumDocFreq = stats.sumDocFreq();
  }

  public void add(CollectionStats stats) {
    this.maxDoc += stats.maxDoc;
    this.docCount += stats.docCount;
    this.sumTotalTermFreq += stats.sumTotalTermFreq;
    this.sumDocFreq += stats.sumDocFreq;
  }
  
  public CollectionStatistics toCollectionStatistics() {
    if (maxDoc == 0 || docCount == 0) {
      return null;
    }
    return new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
  }
  
  public String toString() {
    return StatsUtil.colStatsToString(this);
  }
}
