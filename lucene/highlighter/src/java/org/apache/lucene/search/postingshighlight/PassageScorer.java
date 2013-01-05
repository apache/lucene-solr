package org.apache.lucene.search.postingshighlight;

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

/** 
 * Ranks passages found by {@link PostingsHighlighter}.
 * <p>
 * Each passage is scored as a miniature document within the document.
 * The final score is computed as {@link #norm} * &sum; ({@link #weight} * {@link #tf}).
 * The default implementation is {@link #norm} * BM25.
 * @lucene.experimental
 */
public class PassageScorer {
  
  // TODO: this formula is completely made up. It might not provide relevant snippets!
  
  /** BM25 k1 parameter, controls term frequency normalization */
  public static final float k1 = 1.2f;
  /** BM25 b parameter, controls length normalization. */
  public static final float b = 0.75f;
  
  /**
   * A pivot used for length normalization.
   * The default value is the typical average English sentence length.
   */
  public static final float pivot = 87f;
    
  /**
   * Computes term importance, given its in-document statistics.
   * 
   * @param contentLength length of document in characters
   * @param totalTermFreq number of time term occurs in document
   * @return term importance
   */
  public float weight(int contentLength, int totalTermFreq) {
    // approximate #docs from content length
    float numDocs = 1 + contentLength / pivot;
    // numDocs not numDocs - docFreq (ala DFR), since we approximate numDocs
    return (k1 + 1) * (float) Math.log(1 + (numDocs + 0.5D)/(totalTermFreq + 0.5D));
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
