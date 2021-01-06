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
package org.apache.lucene.search.suggest.document;

import java.io.IOException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.automaton.Automaton;

/**
 * Expert: Responsible for executing the query against an appropriate suggester and collecting the
 * results via a collector.
 *
 * <p>{@link #score(LeafCollector, Bits, int, int)} is called for each leaf reader.
 *
 * <p>{@link #accept(int,Bits)} and {@link #score(float, float)} is called for every matched
 * completion (i.e. document)
 *
 * @lucene.experimental
 */
public class CompletionScorer extends BulkScorer {
  private final NRTSuggester suggester;
  private final Bits filterDocs;

  // values accessed by suggester
  /** weight that created this scorer */
  protected final CompletionWeight weight;

  final LeafReader reader;
  final boolean filtered;
  final Automaton automaton;

  /**
   * Creates a scorer for a field-specific <code>suggester</code> scoped by <code>acceptDocs</code>
   */
  protected CompletionScorer(
      final CompletionWeight weight,
      final NRTSuggester suggester,
      final LeafReader reader,
      final Bits filterDocs,
      final boolean filtered,
      final Automaton automaton)
      throws IOException {
    this.weight = weight;
    this.suggester = suggester;
    this.reader = reader;
    this.automaton = automaton;
    this.filtered = filtered;
    this.filterDocs = filterDocs;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    if (!(collector instanceof TopSuggestDocsCollector)) {
      throw new IllegalArgumentException("collector is not of type TopSuggestDocsCollector");
    }
    suggester.lookup(this, acceptDocs, ((TopSuggestDocsCollector) collector));
    return max;
  }

  @Override
  public long cost() {
    return 0;
  }

  /**
   * Returns true if a document with <code>docID</code> is accepted, false if the docID maps to a
   * deleted document or has been filtered out
   *
   * @param liveDocs the {@link Bits} representing live docs, or possibly {@code null} if all docs
   *     are live
   */
  public final boolean accept(int docID, Bits liveDocs) {
    return (filterDocs == null || filterDocs.get(docID))
        && (liveDocs == null || liveDocs.get(docID));
  }

  /**
   * Returns the score for a matched completion based on the query time boost and the index time
   * weight.
   */
  public float score(float weight, float boost) {
    if (boost == 0f) {
      return weight;
    }
    if (weight == 0f) {
      return boost;
    }
    return weight * boost;
  }
}
