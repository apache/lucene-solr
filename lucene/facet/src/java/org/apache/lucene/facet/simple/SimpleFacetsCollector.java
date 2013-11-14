package org.apache.lucene.facet.simple;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;

// nocommit javadocs
public final class SimpleFacetsCollector extends Collector {

  private AtomicReaderContext context;
  private Scorer scorer;
  private FixedBitSet bits;
  private int totalHits;
  private float[] scores;
  private final boolean keepScores;
  private final List<MatchingDocs> matchingDocs = new ArrayList<MatchingDocs>();
  
  /**
   * Holds the documents that were matched in the {@link AtomicReaderContext}.
   * If scores were required, then {@code scores} is not null.
   */
  public final static class MatchingDocs {
    
    public final AtomicReaderContext context;
    public final FixedBitSet bits;
    public final float[] scores;
    public final int totalHits;
    
    public MatchingDocs(AtomicReaderContext context, FixedBitSet bits, int totalHits, float[] scores) {
      this.context = context;
      this.bits = bits;
      this.scores = scores;
      this.totalHits = totalHits;
    }
  }

  public SimpleFacetsCollector() {
    this(false);
  }

  public SimpleFacetsCollector(boolean keepScores) {
    this.keepScores = keepScores;
  }

  public boolean getKeepScores() {
    return keepScores;
  }
  
  /**
   * Returns the documents matched by the query, one {@link MatchingDocs} per
   * visited segment.
   */
  public List<MatchingDocs> getMatchingDocs() {
    if (bits != null) {
      matchingDocs.add(new MatchingDocs(this.context, bits, totalHits, scores));
      bits = null;
      scores = null;
      context = null;
    }

    return matchingDocs;
  }
    
  @Override
  public final boolean acceptsDocsOutOfOrder() {
    // nocommit why not true?
    return false;
  }

  @Override
  public final void collect(int doc) throws IOException {
    bits.set(doc);
    if (keepScores) {
      if (totalHits >= scores.length) {
        float[] newScores = new float[ArrayUtil.oversize(totalHits + 1, 4)];
        System.arraycopy(scores, 0, newScores, 0, totalHits);
        scores = newScores;
      }
      scores[totalHits] = scorer.score();
    }
    totalHits++;
  }

  @Override
  public final void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }
    
  @Override
  public final void setNextReader(AtomicReaderContext context) throws IOException {
    if (bits != null) {
      matchingDocs.add(new MatchingDocs(this.context, bits, totalHits, scores));
    }
    bits = new FixedBitSet(context.reader().maxDoc());
    totalHits = 0;
    if (keepScores) {
      scores = new float[64]; // some initial size
    }
    this.context = context;
  }
}
