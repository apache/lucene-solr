package org.apache.lucene.search;

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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.Bits;

// TODO: we could also have an ExpressionRescorer

/** A {@link Rescorer} that uses a provided Query to assign
 *  scores to the first-pass hits.
 *
 * @lucene.experimental */
public abstract class QueryRescorer extends Rescorer {

  private final Query query;

  /** Sole constructor, passing the 2nd pass query to
   *  assign scores to the 1st pass hits.  */
  public QueryRescorer(Query query) {
    this.query = query;
  }

  /**
   * Implement this in a subclass to combine the first pass and
   * second pass scores.  If secondPassMatches is false then
   * the second pass query failed to match a hit from the
   * first pass query, and you should ignore the
   * secondPassScore.
   */
  protected abstract float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore);

  @Override
  public TopDocs rescore(IndexSearcher searcher, TopDocs topDocs, int topN) throws IOException {
    int[] docIDs = new int[topDocs.scoreDocs.length];
    for(int i=0;i<docIDs.length;i++) {
      docIDs[i] = topDocs.scoreDocs[i].doc;
    }

    TopDocs topDocs2 = searcher.search(query, new OnlyDocIDsFilter(docIDs), topDocs.scoreDocs.length);

    // TODO: we could save small young GC cost here if we
    // cloned the incoming ScoreDoc[], sorted that by doc,
    // passed that to OnlyDocIDsFilter, sorted 2nd pass
    // TopDocs by doc, did a merge sort to combine the
    // scores, and finally re-sorted by the combined score,
    // but that is sizable added code complexity for minor
    // GC savings:
    Map<Integer,Float> newScores = new HashMap<Integer,Float>();
    for(ScoreDoc sd : topDocs2.scoreDocs) {
      newScores.put(sd.doc, sd.score);
    }

    ScoreDoc[] newHits = new ScoreDoc[topDocs.scoreDocs.length];
    for(int i=0;i<topDocs.scoreDocs.length;i++) {
      ScoreDoc sd = topDocs.scoreDocs[i];
      Float newScore = newScores.get(sd.doc);
      float combinedScore;
      if (newScore == null) {
        combinedScore = combine(sd.score, false, 0.0f);
      } else {
        combinedScore = combine(sd.score, true, newScore.floatValue());
      }
      newHits[i] = new ScoreDoc(sd.doc, combinedScore);
    }

    // TODO: we should do a partial sort (of only topN)
    // instead, but typically the number of hits is
    // smallish:
    Arrays.sort(newHits,
                new Comparator<ScoreDoc>() {
                  @Override
                  public int compare(ScoreDoc a, ScoreDoc b) {
                    // Sort by score descending, then docID ascending:
                    if (a.score > b.score) {
                      return -1;
                    } else if (a.score < b.score) {
                      return 1;
                    } else {
                      // This subtraction can't overflow int
                      // because docIDs are >= 0:
                      return a.doc - b.doc;
                    }
                  }
                });

    if (topN < newHits.length) {
      ScoreDoc[] subset = new ScoreDoc[topN];
      System.arraycopy(newHits, 0, subset, 0, topN);
      newHits = subset;
    }

    return new TopDocs(topDocs.totalHits, newHits, newHits[0].score);
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException {
    Explanation secondPassExplanation = searcher.explain(query, docID);

    Float secondPassScore = secondPassExplanation.isMatch() ? secondPassExplanation.getValue() : null;

    float score;
    if (secondPassScore == null) {
      score = combine(firstPassExplanation.getValue(), false, 0.0f);
    } else {
      score = combine(firstPassExplanation.getValue(), true,  secondPassScore.floatValue());
    }

    Explanation result = new Explanation(score, "combined first and second pass score using " + getClass());

    Explanation first = new Explanation(firstPassExplanation.getValue(), "first pass score");
    first.addDetail(firstPassExplanation);
    result.addDetail(first);

    Explanation second;
    if (secondPassScore == null) {
      second = new Explanation(0.0f, "no second pass score");
    } else {
      second = new Explanation(secondPassScore, "second pass score");
    }
    second.addDetail(secondPassExplanation);
    result.addDetail(second);

    return result;
  }

  /** Sugar API, calling {#rescore} using a simple linear
   *  combination of firstPassScore + weight * secondPassScore */
  public static TopDocs rescore(IndexSearcher searcher, TopDocs topDocs, Query query, final double weight, int topN) throws IOException {
    return new QueryRescorer(query) {
      @Override
      protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
        float score = firstPassScore;
        if (secondPassMatches) {
          score += weight * secondPassScore;
        }
        return score;
      }
    }.rescore(searcher, topDocs, topN);
  }

  /** Filter accepting only the specified docIDs */
  private static class OnlyDocIDsFilter extends Filter {

    private final int[] docIDs;

    /** Sole constructor. */
    public OnlyDocIDsFilter(int[] docIDs) {
      this.docIDs = docIDs;
      Arrays.sort(docIDs);
    }

    @Override
    public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      int loc = Arrays.binarySearch(docIDs, context.docBase);
      if (loc < 0) {
        loc = -loc-1;
      }

      final int startLoc = loc;
      final int endDoc = context.docBase + context.reader().maxDoc();

      return new DocIdSet() {

        int pos = startLoc;

        @Override
        public DocIdSetIterator iterator() throws IOException {
          return new DocIdSetIterator() {

            int docID;

            @Override
            public int docID() {
              return docID;
            }

            @Override
            public int nextDoc() {
              if (pos == docIDs.length) {
                return NO_MORE_DOCS;
              }
              int docID = docIDs[pos];
              if (docID >= endDoc) {
                return NO_MORE_DOCS;
              }
              pos++;
              assert acceptDocs == null || acceptDocs.get(docID-context.docBase);
              return docID-context.docBase;
            }

            @Override
            public long cost() {
              // NOTE: not quite right, since this is cost
              // across all segments, and we are supposed to
              // return cost for just this segment:
              return docIDs.length;
            }

            @Override
            public int advance(int target) {
              // TODO: this is a full binary search; we
              // could optimize (a bit) by setting lower
              // bound to current pos instead:
              int loc = Arrays.binarySearch(docIDs, target + context.docBase);
              if (loc < 0) {
                loc = -loc-1;
              }
              pos = loc;
              return nextDoc();
            }
          };
        }
      };
    }
  }
}
