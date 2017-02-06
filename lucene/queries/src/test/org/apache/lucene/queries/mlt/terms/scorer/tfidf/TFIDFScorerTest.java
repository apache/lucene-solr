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

package org.apache.lucene.queries.mlt.terms.scorer.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.mlt.terms.scorer.BM25Scorer;
import org.apache.lucene.queries.mlt.terms.scorer.TFIDFScorer;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

public class TFIDFScorerTest {
  public static final String SAMPLE_FIELD = "field1";
  public static final String SAMPLE_TERM = "term1";

  private TFIDFScorer scorerToTest = new TFIDFScorer();

  @Test
  public void sampleStats_shouldCalculateTFIDFSimilarityScore() throws IOException {
    CollectionStatistics field1Stats = new CollectionStatistics(SAMPLE_FIELD, 100, 90, 1000, 1000);

    BytesRef term1ByteRef = new BytesRef(SAMPLE_TERM);
    TermStatistics term1Stat = new TermStatistics(term1ByteRef, 20, 60);

    float score = scorerToTest.score(SAMPLE_FIELD, field1Stats, term1Stat, 20);
    Assert.assertEquals(49.30, score, 0.03);
  }


}
