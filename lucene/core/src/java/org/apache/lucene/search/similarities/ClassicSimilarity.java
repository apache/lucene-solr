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
package org.apache.lucene.search.similarities;


import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;

/**
 * Expert: Historical scoring implementation. You might want to consider using
 * {@link BM25Similarity} instead, which is generally considered superior to
 * TF-IDF.
 */
public class ClassicSimilarity extends TFIDFSimilarity {

  /** Sole constructor: parameter-free */
  public ClassicSimilarity() {}

  /** Implemented as
   *  <code>1/sqrt(length)</code>.
   *
   *  @lucene.experimental */
  @Override
  public float lengthNorm(int numTerms) {
    return (float) (1.0 / Math.sqrt(numTerms));
  }

  /** Implemented as <code>sqrt(freq)</code>. */
  @Override
  public float tf(float freq) {
    return (float)Math.sqrt(freq);
  }
    
  /** Implemented as <code>1 / (distance + 1)</code>. */
  @Override
  public float sloppyFreq(int distance) {
    return 1.0f / (distance + 1);
  }
  
  /** The default implementation returns <code>1</code> */
  @Override
  public float scorePayload(int doc, int start, int end, BytesRef payload) {
    return 1;
  }

  @Override
  public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
    final long df = termStats.docFreq();
    final long docCount = collectionStats.docCount() == -1 ? collectionStats.maxDoc() : collectionStats.docCount();
    final float idf = idf(df, docCount);
    return Explanation.match(idf, "idf, computed as log((docCount+1)/(docFreq+1)) + 1 from:",
        Explanation.match(df, "docFreq"),
        Explanation.match(docCount, "docCount"));
  }

  /** Implemented as <code>log((docCount+1)/(docFreq+1)) + 1</code>. */
  @Override
  public float idf(long docFreq, long docCount) {
    return (float)(Math.log((docCount+1)/(double)(docFreq+1)) + 1.0);
  }

  @Override
  public String toString() {
    return "ClassicSimilarity";
  }
}
