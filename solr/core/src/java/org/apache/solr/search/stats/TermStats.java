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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermStatistics;

/**
 * Modifiable version of {@link TermStatistics} useful for aggregation of
 * per-shard stats.
 */
public class TermStats {
  final public String term;
  public long docFreq;
  public long totalTermFreq;
  private final Term t;
  
  public TermStats(String term) {
    this.term = term;
    t = makeTerm(term);
  }

  private Term makeTerm(String s) {
    int idx = s.indexOf(':');
    if (idx == -1) {
      return null;
    }
    return new Term(s.substring(0, idx), s.substring(idx + 1));
  }
  
  public TermStats(String term, long docFreq, long totalTermFreq) {
    this(term);
    this.docFreq = docFreq;
    this.totalTermFreq = totalTermFreq;
  }
  
  public TermStats(String field, TermStatistics stats) {
    this.term = field + ":" + stats.term().utf8ToString();
    this.t = new Term(field, stats.term());
    this.docFreq = stats.docFreq();
    this.totalTermFreq = stats.totalTermFreq();
  }
  
  public void add(TermStats stats) {
    this.docFreq += stats.docFreq;
    this.totalTermFreq += stats.totalTermFreq;
  }
  
  public TermStatistics toTermStatistics() {
    if (docFreq == 0) {
      return null;
    }
    return new TermStatistics(t.bytes(), docFreq, totalTermFreq);
  }
  
  public String toString() {
    return StatsUtil.termStatsToString(this, false);
  }
}
