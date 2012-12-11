package org.apache.lucene.sandbox.postingshighlight;

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

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;

/** 
 * Used for ranking passages.
 * <p>
 * Each passage is scored as a miniature document within the document.
 * The final score is computed as {@link #norm} * {@link #weight} * &sum; {@link #tf}.
 * The default implementation is BM25 * {@link #norm}.
 * @lucene.experimental
 */
public class PassageScorer {
  
  // TODO: this formula completely made up. It might not provide relevant snippets!
  
  /** BM25 k1 parameter, controls term frequency normalization */
  public static final float k1 = 1.2f;
  /** BM25 b parameter, controls length normalization. */
  public static final float b = 0.75f;
  
  /**
   * A pivot used for length normalization.
   * The default value is the typical average english sentence length.
   */
  public static final float pivot = 87f;
    
  /**
   * Computes term importance, given its collection-wide statistics.
   * 
   * @param collectionStats statistics for the collection
   * @param termStats statistics for the term
   * @return term importance
   */
  public float weight(CollectionStatistics collectionStats, TermStatistics termStats) {
    long numDocs = collectionStats.maxDoc();
    long docFreq = termStats.docFreq();
    return (k1 + 1) * (float) Math.log(1 + (numDocs - docFreq + 0.5D)/(docFreq + 0.5D));
  }

  /**
   * Computes term weight, given the frequency within the passage
   * and the passage's length.
   * 
   * @param freq number of occurrences of within this passage
   * @param passageLen length of the passage in characters.
   * @return term weight
   */
  public float tf(int freq, int passageLen) {
    float norm = k1 * ((1 - b) + b * (passageLen / pivot));
    return freq / (freq + norm);
  }
    
  /**
   * Normalize a passage according to its position in the document.
   * <p>
   * Typically passages towards the beginning of the document are 
   * more useful for summarizing the contents.
   * <p>
   * The default implementation is <code>1 + 1/log(pivot + passageStart)</code>
   * @param passageStart start offset of the passage
   * @return a boost value multiplied into the passage's core.
   */
  public float norm(int passageStart) {
    return 1 + 1/(float)Math.log(pivot + passageStart);
  }
}
