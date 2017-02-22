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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.SimpleCollector;

import static org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;

/**
 * {@link org.apache.lucene.search.Collector} that collects completion and
 * score, along with document id
 * <p>
 * Non scoring collector that collect completions in order of their
 * pre-computed scores.
 * <p>
 * NOTE: One document can be collected multiple times if a document
 * is matched for multiple unique completions for a given query
 * <p>
 * Subclasses should only override
 * {@link TopSuggestDocsCollector#collect(int, CharSequence, CharSequence, float)}.
 * <p>
 * NOTE: {@link #setScorer(org.apache.lucene.search.Scorer)} and
 * {@link #collect(int)} is not used
 *
 * @lucene.experimental
 */
public class TopSuggestDocsCollector extends SimpleCollector {

  private final SuggestScoreDocPriorityQueue priorityQueue;
  private final int num;

  /** Only set if we are deduplicating hits: holds all per-segment hits until the end, when we dedup them */
  private final List<SuggestScoreDoc> pendingResults;

  /** Only set if we are deduplicating hits: holds all surface forms seen so far in the current segment */
  final CharArraySet seenSurfaceForms;

  /** Document base offset for the current Leaf */
  protected int docBase;

  /**
   * Sole constructor
   *
   * Collects at most <code>num</code> completions
   * with corresponding document and weight
   */
  public TopSuggestDocsCollector(int num, boolean skipDuplicates) {
    if (num <= 0) {
      throw new IllegalArgumentException("'num' must be > 0");
    }
    this.num = num;
    this.priorityQueue = new SuggestScoreDocPriorityQueue(num);
    if (skipDuplicates) {
      seenSurfaceForms = new CharArraySet(num, false);
      pendingResults = new ArrayList<>();
    } else {
      seenSurfaceForms = null;
      pendingResults = null;
    }
  }

  /** Returns true if duplicates are filtered out */
  protected boolean doSkipDuplicates() {
    return seenSurfaceForms != null;
  }

  /**
   * Returns the number of results to be collected
   */
  public int getCountToCollect() {
    return num;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    docBase = context.docBase;
    if (seenSurfaceForms != null) {
      seenSurfaceForms.clear();
      // NOTE: this also clears the priorityQueue:
      for (SuggestScoreDoc hit : priorityQueue.getResults()) {
        pendingResults.add(hit);
      }
    }
  }

  /**
   * Called for every matched completion,
   * similar to {@link org.apache.lucene.search.LeafCollector#collect(int)}
   * but for completions.
   *
   * NOTE: collection at the leaf level is guaranteed to be in
   * descending order of score
   */
  public void collect(int docID, CharSequence key, CharSequence context, float score) throws IOException {
    SuggestScoreDoc current = new SuggestScoreDoc(docBase + docID, key, context, score);
    if (current == priorityQueue.insertWithOverflow(current)) {
      // if the current SuggestScoreDoc has overflown from pq,
      // we can assume all of the successive collections from
      // this leaf will be overflown as well
      // TODO: reuse the overflow instance?
      throw new CollectionTerminatedException();
    }
  }

  /**
   * Returns at most <code>num</code> Top scoring {@link org.apache.lucene.search.suggest.document.TopSuggestDocs}s
   */
  public TopSuggestDocs get() throws IOException {

    SuggestScoreDoc[] suggestScoreDocs;
    
    if (seenSurfaceForms != null) {
      // NOTE: this also clears the priorityQueue:
      for (SuggestScoreDoc hit : priorityQueue.getResults()) {
        pendingResults.add(hit);
      }

      // Deduplicate all hits: we already dedup'd efficiently within each segment by
      // truncating the FST top paths search, but across segments there may still be dups:
      seenSurfaceForms.clear();

      // TODO: we could use a priority queue here to make cost O(N * log(num)) instead of O(N * log(N)), where N = O(num *
      // numSegments), but typically numSegments is smallish and num is smallish so this won't matter much in practice:

      Collections.sort(pendingResults,
                       new Comparator<SuggestScoreDoc>() {
                         @Override
                         public int compare(SuggestScoreDoc a, SuggestScoreDoc b) {
                           // sort by higher score
                           int cmp = Float.compare(b.score, a.score);
                           if (cmp == 0) {
                             // tie break by lower docID:
                             cmp = Integer.compare(a.doc, b.doc);
                           }
                           return cmp;
                         }
                       });

      List<SuggestScoreDoc> hits = new ArrayList<>();
      
      for (SuggestScoreDoc hit : pendingResults) {
        if (seenSurfaceForms.contains(hit.key) == false) {
          seenSurfaceForms.add(hit.key);
          hits.add(hit);
          if (hits.size() == num) {
            break;
          }
        }
      }
      suggestScoreDocs = hits.toArray(new SuggestScoreDoc[0]);
    } else {
      suggestScoreDocs = priorityQueue.getResults();
    }

    if (suggestScoreDocs.length > 0) {
      return new TopSuggestDocs(suggestScoreDocs.length, suggestScoreDocs, suggestScoreDocs[0].score);
    } else {
      return TopSuggestDocs.EMPTY;
    }
  }

  /**
   * Ignored
   */
  @Override
  public void collect(int doc) throws IOException {
    // {@link #collect(int, CharSequence, CharSequence, long)} is used
    // instead
  }

  /**
   * Ignored
   */
  @Override
  public boolean needsScores() {
    return true;
  }
}
