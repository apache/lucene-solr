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
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BM25Similarity.BM25DocScorer;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Implementation using BM25 {@link BM25Similarity} to calculate term score .
 */
public class BM25Scorer implements TermScorer {

  private BM25Similarity similarity = new BM25Similarity();

  private Map<String, NumericDocValues> field2normsFromIndex;
  private Map<String, Float> field2norm;
  private int docId;
  private float textNorm;

  @Override
  public float score(String fieldName, CollectionStatistics fieldStats, TermStatistics termStats, float termFrequency) throws IOException {
    float termScore = 0;
    Similarity.SimWeight bm25SimilarityStats = similarity.computeWeight(1.0f, fieldStats, termStats);

    BM25DocScorer similarityScorer;

    boolean scoringLocalTerm = field2normsFromIndex != null;
    boolean scoringCloudTerm = field2norm!=null;

    if (scoringLocalTerm) {
      similarityScorer = similarity.instantiateSimilarityScorer(bm25SimilarityStats, field2normsFromIndex.get(fieldName));
      termScore = similarityScorer.score(docId, termFrequency);
    } else if(scoringCloudTerm){
      similarityScorer = similarity.instantiateSimilarityScorer(bm25SimilarityStats,null);
      termScore = similarityScorer.score(termFrequency, field2norm.get(fieldName));
    } else{
      similarityScorer = similarity.instantiateSimilarityScorer(bm25SimilarityStats,null);
      termScore = similarityScorer.score(termFrequency, textNorm);
    }
    return termScore;

  }

  public Similarity.SimWeight getSimilarityStats(String fieldName, CollectionStatistics fieldStats, TermStatistics termStats, float termFrequency) throws IOException {
    return similarity.computeWeight(1.0f, fieldStats, termStats);
  }

  public void setField2normsFromIndex(Map<String, NumericDocValues> field2normsFromIndex) {
    this.field2normsFromIndex = field2normsFromIndex;
  }

  public void setField2norm(Map<String, Float> field2norm) {
    this.field2norm = field2norm;
  }

  public int getDocId() {
    return docId;
  }

  public void setDocId(int docId) {
    this.docId = docId;
  }

  public void setTextNorm(float textNorm) {
    this.textNorm = textNorm;
  }
}
