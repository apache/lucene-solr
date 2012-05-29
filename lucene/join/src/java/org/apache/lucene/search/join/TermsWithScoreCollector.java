package org.apache.lucene.search.join;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

import java.io.IOException;

abstract class TermsWithScoreCollector extends Collector {

  private final static int INITIAL_ARRAY_SIZE = 256;

  final String field;
  final BytesRefHash collectedTerms = new BytesRefHash();
  final ScoreMode scoreMode;

  Scorer scorer;
  float[] scoreSums = new float[INITIAL_ARRAY_SIZE];

  TermsWithScoreCollector(String field, ScoreMode scoreMode) {
    this.field = field;
    this.scoreMode = scoreMode;
  }

  public BytesRefHash getCollectedTerms() {
    return collectedTerms;
  }

  public float[] getScoresPerTerm() {
    return scoreSums;
  }

  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }

  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  /**
   * Chooses the right {@link TermsWithScoreCollector} implementation.
   *
   * @param field                     The field to collect terms for
   * @param multipleValuesPerDocument Whether the field to collect terms for has multiple values per document.
   * @return a {@link TermsWithScoreCollector} instance
   */
  static TermsWithScoreCollector create(String field, boolean multipleValuesPerDocument, ScoreMode scoreMode) {
    if (multipleValuesPerDocument) {
      switch (scoreMode) {
        case Avg:
          return new MV.Avg(field);
        default:
          return new MV(field, scoreMode);
      }
    } else {
      switch (scoreMode) {
        case Avg:
          return new SV.Avg(field);
        default:
          return new SV(field, scoreMode);
      }
    }
  }

  // impl that works with single value per document
  static class SV extends TermsWithScoreCollector {

    final BytesRef spare = new BytesRef();
    FieldCache.DocTerms fromDocTerms;

    SV(String field, ScoreMode scoreMode) {
      super(field, scoreMode);
    }

    public void collect(int doc) throws IOException {
      int ord = collectedTerms.add(fromDocTerms.getTerm(doc, spare));
      if (ord < 0) {
        ord = -ord - 1;
      } else {
        if (ord >= scoreSums.length) {
          scoreSums = ArrayUtil.grow(scoreSums);
        }
      }

      float current = scorer.score();
      float existing = scoreSums[ord];
      if (Float.compare(existing, 0.0f) == 0) {
        scoreSums[ord] = current;
      } else {
        switch (scoreMode) {
          case Total:
            scoreSums[ord] = scoreSums[ord] + current;
            break;
          case Max:
            if (current > existing) {
              scoreSums[ord] = current;
            }
        }
      }
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      fromDocTerms = FieldCache.DEFAULT.getTerms(context.reader(), field);
    }

    static class Avg extends SV {

      int[] scoreCounts = new int[INITIAL_ARRAY_SIZE];

      Avg(String field) {
        super(field, ScoreMode.Avg);
      }

      @Override
      public void collect(int doc) throws IOException {
        int ord = collectedTerms.add(fromDocTerms.getTerm(doc, spare));
        if (ord < 0) {
          ord = -ord - 1;
        } else {
          if (ord >= scoreSums.length) {
            scoreSums = ArrayUtil.grow(scoreSums);
            scoreCounts = ArrayUtil.grow(scoreCounts);
          }
        }

        float current = scorer.score();
        float existing = scoreSums[ord];
        if (Float.compare(existing, 0.0f) == 0) {
          scoreSums[ord] = current;
          scoreCounts[ord] = 1;
        } else {
          scoreSums[ord] = scoreSums[ord] + current;
          scoreCounts[ord]++;
        }
      }

      @Override
      public float[] getScoresPerTerm() {
        if (scoreCounts != null) {
          for (int i = 0; i < scoreCounts.length; i++) {
            scoreSums[i] = scoreSums[i] / scoreCounts[i];
          }
          scoreCounts = null;
        }
        return scoreSums;
      }
    }
  }

  // impl that works with multiple values per document
  static class MV extends TermsWithScoreCollector {

    DocTermOrds fromDocTermOrds;
    TermsEnum docTermsEnum;
    DocTermOrds.TermOrdsIterator reuse;

    MV(String field, ScoreMode scoreMode) {
      super(field, scoreMode);
    }

    public void collect(int doc) throws IOException {
      reuse = fromDocTermOrds.lookup(doc, reuse);
      int[] buffer = new int[5];

      int chunk;
      do {
        chunk = reuse.read(buffer);
        if (chunk == 0) {
          return;
        }

        for (int idx = 0; idx < chunk; idx++) {
          int key = buffer[idx];
          docTermsEnum.seekExact((long) key);
          int ord = collectedTerms.add(docTermsEnum.term());
          if (ord < 0) {
            ord = -ord - 1;
          } else {
            if (ord >= scoreSums.length) {
              scoreSums = ArrayUtil.grow(scoreSums);
            }
          }

          final float current = scorer.score();
          final float existing = scoreSums[ord];
          if (Float.compare(existing, 0.0f) == 0) {
            scoreSums[ord] = current;
          } else {
            switch (scoreMode) {
              case Total:
                scoreSums[ord] = existing + current;
                break;
              case Max:
                if (current > existing) {
                  scoreSums[ord] = current;
                }
            }
          }
        }
      } while (chunk >= buffer.length);
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      fromDocTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), field);
      docTermsEnum = fromDocTermOrds.getOrdTermsEnum(context.reader());
      reuse = null; // LUCENE-3377 needs to be fixed first then this statement can be removed...
    }

    static class Avg extends MV {

      int[] scoreCounts = new int[INITIAL_ARRAY_SIZE];

      Avg(String field) {
        super(field, ScoreMode.Avg);
      }

      @Override
      public void collect(int doc) throws IOException {
        reuse = fromDocTermOrds.lookup(doc, reuse);
        int[] buffer = new int[5];

        int chunk;
        do {
          chunk = reuse.read(buffer);
          if (chunk == 0) {
            return;
          }

          for (int idx = 0; idx < chunk; idx++) {
            int key = buffer[idx];
            docTermsEnum.seekExact((long) key);
            int ord = collectedTerms.add(docTermsEnum.term());
            if (ord < 0) {
              ord = -ord - 1;
            } else {
              if (ord >= scoreSums.length) {
                scoreSums = ArrayUtil.grow(scoreSums);
                scoreCounts = ArrayUtil.grow(scoreCounts);
              }
            }

            float current = scorer.score();
            float existing = scoreSums[ord];
            if (Float.compare(existing, 0.0f) == 0) {
              scoreSums[ord] = current;
              scoreCounts[ord] = 1;
            } else {
              scoreSums[ord] = scoreSums[ord] + current;
              scoreCounts[ord]++;
            }
          }
        } while (chunk >= buffer.length);
      }

      @Override
      public float[] getScoresPerTerm() {
        if (scoreCounts != null) {
          for (int i = 0; i < scoreCounts.length; i++) {
            scoreSums[i] = scoreSums[i] / scoreCounts[i];
          }
          scoreCounts = null;
        }
        return scoreSums;
      }
    }
  }

}
