package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Expert: Returned by low-level search implementations.
 * @see Searcher#search(Query,Filter,int) */
public class TopDocs implements java.io.Serializable {
  /** Expert: The total number of hits for the query.
   * @see Hits#length()
  */
  public int totalHits;
  /** Expert: The top hits for the query. */
  public ScoreDoc[] scoreDocs;
  /** Expert: Stores the maximum score value encountered, needed for normalizing. */
  private float maxScore;
  
  /** Expert: Returns the maximum score value encountered. */
  public float getMaxScore() {
      return maxScore;
  }
  
  /** Expert: Sets the maximum score value encountered. */
  public void setMaxScore(float maxScore) {
      this.maxScore=maxScore;
  }
  
  /** Expert: Constructs a TopDocs.*/
  TopDocs(int totalHits, ScoreDoc[] scoreDocs, float maxScore) {
    this.totalHits = totalHits;
    this.scoreDocs = scoreDocs;
    this.maxScore = maxScore;
  }
}
