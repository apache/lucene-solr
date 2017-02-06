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

package org.apache.lucene.queries.mlt.terms.scorer;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;

/**
 * Implementation using BM25 {@link TFIDFSimilarity} to calculate term score .
 */
public class TFIDFScorer implements TermScorer {
  TFIDFSimilarity similarity = new ClassicSimilarity();

  @Override
  public float score(String fieldName, CollectionStatistics fieldStats, TermStatistics termStats, float termFrequency) throws IOException {
    float idf = similarity.idf(termStats.docFreq(), fieldStats.docCount());
    float score = termFrequency * idf;
    return score;
  }

  public Similarity.SimWeight getSimilarityStats(String fieldName, CollectionStatistics fieldStats, TermStatistics termStats, float termFrequency) throws IOException {
    return similarity.computeWeight(1.0f, fieldStats, termStats);
  }

  @Override
  public void setField2normsFromIndex(Map<String, NumericDocValues> field2normsFromIndex) {

  }

  @Override
  public void setField2norm(Map<String, Float> field2norm) {

  }

  @Override
  public void setDocId(int docId) {

  }

  @Override
  public void setTextNorm(float textNorm) {

  }

}
