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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.fst.Util.TopNSearcher;

/**
 * {@link org.apache.lucene.search.TopDocs} wrapper with
 * an additional CharSequence key per {@link org.apache.lucene.search.ScoreDoc}
 *
 * @lucene.experimental
 */
public class TopSuggestDocs extends TopDocs {

  /**
   * Singleton for empty {@link TopSuggestDocs}
   */
  public final static TopSuggestDocs EMPTY = new TopSuggestDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO),
      new SuggestScoreDoc[0], true);

  /**
   * {@link org.apache.lucene.search.ScoreDoc} with an
   * additional CharSequence key
   */
  public static class SuggestScoreDoc extends ScoreDoc implements Comparable<SuggestScoreDoc> {

    /**
     * Matched completion key
     */
    public final CharSequence key;

    /**
     * Context for the completion
     */
    public final CharSequence context;

    /**
     * Creates a SuggestScoreDoc instance
     *
     * @param doc   document id (hit)
     * @param key   matched completion
     * @param score weight of the matched completion
     */
    public SuggestScoreDoc(int doc, CharSequence key, CharSequence context, float score) {
      super(doc, score);
      this.key = key;
      this.context = context;
    }

    @Override
    public int compareTo(SuggestScoreDoc o) {
      return Lookup.CHARSEQUENCE_COMPARATOR.compare(key, o.key);
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof SuggestScoreDoc == false) {
        return false;
      } else {
        return key.equals(((SuggestScoreDoc) other).key);
      }
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }

    @Override
    public String toString() {
      return "key=" + key + " doc=" + doc + " score=" + score + " shardIndex=" + shardIndex;      
    }
  }

  /**
   * Indicates that all possibilities for completions were exhausted
   */
  final boolean isComplete;

  /**
   * {@link org.apache.lucene.search.TopDocs} wrapper with
   * {@link TopSuggestDocs.SuggestScoreDoc}
   * instead of {@link org.apache.lucene.search.ScoreDoc}
   */
  public TopSuggestDocs(TotalHits totalHits, SuggestScoreDoc[] scoreDocs, boolean isComplete) {
    super(totalHits, scoreDocs);
    this.isComplete = isComplete;
  }

  public TopSuggestDocs(TotalHits totalHits, SuggestScoreDoc[] scoreDocs) {
    this(totalHits, scoreDocs, true);
  }

  /**
   * Returns {@link TopSuggestDocs.SuggestScoreDoc}s
   * for this instance
   */
  public SuggestScoreDoc[] scoreLookupDocs() {
    return (SuggestScoreDoc[]) scoreDocs;
  }

  /**
   * Returns a new TopSuggestDocs, containing topN results across
   * the provided TopSuggestDocs, sorting by score. Each {@link TopSuggestDocs}
   * instance must be sorted.
   * Analogous to {@link org.apache.lucene.search.TopDocs#merge(int, org.apache.lucene.search.TopDocs[])}
   * for {@link TopSuggestDocs}
   *
   * NOTE: assumes every <code>shardHit</code> is already sorted by score
   */
  public static TopSuggestDocs merge(int topN, TopSuggestDocs[] shardHits) {
    SuggestScoreDocPriorityQueue priorityQueue = new SuggestScoreDocPriorityQueue(topN);
    boolean allComplete = true;
    for (TopSuggestDocs shardHit : shardHits) {
      for (SuggestScoreDoc scoreDoc : shardHit.scoreLookupDocs()) {
        if (scoreDoc == priorityQueue.insertWithOverflow(scoreDoc)) {
          break;
        }
      }
      allComplete &= shardHit.isComplete;
    }
    SuggestScoreDoc[] topNResults = priorityQueue.getResults();
    if (topNResults.length > 0) {
      return new TopSuggestDocs(new TotalHits(topNResults.length, TotalHits.Relation.EQUAL_TO), topNResults,
          allComplete);
    } else {
      return TopSuggestDocs.EMPTY;
    }
  }

  /**
   * Indicates if the list of results is complete or not. Might be <code>false</code> if the {@link TopNSearcher} rejected
   * too many of the queued results.
   */
  public boolean isComplete() {
    return this.isComplete;
  }
}
