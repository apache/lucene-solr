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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ArrayUtil;

/**
 * Query wrapper that reduces the size of max-score blocks to more easily detect
 * problems with the max-score logic.
 */
public final class BlockScoreQueryWrapper extends Query {

  private final Query query;
  private final int blockLength;

  /** Sole constructor. */
  public BlockScoreQueryWrapper(Query query, int blockLength) {
    this.query = Objects.requireNonNull(query);
    this.blockLength = blockLength;
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    BlockScoreQueryWrapper that = (BlockScoreQueryWrapper) obj;
    return Objects.equals(query, that.query) && blockLength == that.blockLength;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + query.hashCode();
    h = 31 * h + blockLength;
    return h;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = query.rewrite(reader);
    if (rewritten != query) {
      return new BlockScoreQueryWrapper(rewritten, blockLength);
    }
    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final Weight inWeight = query.createWeight(searcher, scoreMode, boost);
    if (scoreMode.needsScores() == false) {
      return inWeight;
    }
    return new Weight(this) {
      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return inWeight.isCacheable(ctx);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        Scorer inScorer = inWeight.scorer(context);
        if (inScorer == null) {
          return null;
        }

        int[] tmpDocs = new int[2];
        float[] tmpScores = new float[2];
        tmpDocs[0] = -1;
        DocIdSetIterator it = inScorer.iterator();
        int i = 1;
        for (int doc = it.nextDoc(); ; doc = it.nextDoc()) {
          tmpDocs = ArrayUtil.grow(tmpDocs, i + 1);
          tmpScores = ArrayUtil.grow(tmpScores, i + 1);
          tmpDocs[i] = doc;
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            i++;
            break;
          }
          tmpScores[i] = inScorer.score();
          i++;
        }
        final int[] docs = ArrayUtil.copyOfSubArray(tmpDocs, 0, i);
        final float[] scores = ArrayUtil.copyOfSubArray(tmpScores, 0, i);

        return new Scorer(inWeight) {

          int i = 0;

          @Override
          public int docID() {
            return docs[i];
          }

          @Override
          public float score() throws IOException {
            return scores[i];
          }

          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {

              @Override
              public int nextDoc() throws IOException {
                assert docs[i] != NO_MORE_DOCS;
                return docs[++i];
              }

              @Override
              public int docID() {
                return docs[i];
              }

              @Override
              public long cost() {
                return docs.length - 2;
              }

              @Override
              public int advance(int target) throws IOException {
                i = Arrays.binarySearch(docs, target);
                if (i < 0) {
                  i = -1 - i;
                }
                assert docs[i] >= target;
                return docs[i];
              }
            };
          }

          private int startOfBlock(int target) {
            int i = Arrays.binarySearch(docs, target);
            if (i < 0) {
              i = -1 - i;
            }
            return i - i % blockLength;
          }

          private int endOfBlock(int target) {
            return Math.min(startOfBlock(target) + blockLength, docs.length - 1);
          }

          int lastShallowTarget = -1;

          @Override
          public int advanceShallow(int target) throws IOException {
            lastShallowTarget = target;
            if (target == DocIdSetIterator.NO_MORE_DOCS) {
              return DocIdSetIterator.NO_MORE_DOCS;
            }
            return docs[endOfBlock(target)] - 1;
          }

          @Override
          public float getMaxScore(int upTo) throws IOException {
            float max = 0;
            for (int j = startOfBlock(Math.max(docs[i], lastShallowTarget)); ; ++j) {
              if (docs[j] > upTo) {
                break;
              }
              max = Math.max(max, scores[j]);
              if (j == docs.length - 1) {
                break;
              }
            }
            return max;
          }

        };
      }

      @Override
      public void extractTerms(Set<Term> terms) {
        inWeight.extractTerms(terms);
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return inWeight.explain(context, doc);
      }
    };
  }
}
