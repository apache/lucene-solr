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
import java.util.Collection;
import java.util.Collections;

/**
 * Allows access to the score of a Query
 */
public abstract class Scorable {

  /**
   * Returns the score of the current document matching the query.
   */
  public abstract float score() throws IOException;

  /**
   * Returns the smoothing score of the current document matching the query. This score is used when
   * the query/term does not appear in the document, and behaves like an idf. The smoothing score is
   * particularly important when the Scorer returns a product of probabilities so that the document
   * score does not go to zero when one probability is zero. This can return 0 or a smoothing score.
   *
   * <p>Smoothing scores are described in many papers, including: Metzler, D. and Croft, W. B. ,
   * "Combining the Language Model and Inference Network Approaches to Retrieval," Information
   * Processing and Management Special Issue on Bayesian Networks and Information Retrieval, 40(5),
   * pp.735-750.
   */
  public float smoothingScore(int docId) throws IOException {
    return 0f;
  }

  /** Returns the doc ID that is currently being scored. */
  public abstract int docID();

  /**
   * Optional method: Tell the scorer that its iterator may safely ignore all
   * documents whose score is less than the given {@code minScore}. This is a
   * no-op by default.
   *
   * This method may only be called from collectors that use
   * {@link ScoreMode#TOP_SCORES}, and successive calls may only set increasing
   * values of {@code minScore}.
   */
  public void setMinCompetitiveScore(float minScore) throws IOException {
    // no-op by default
  }

  /**
   * Returns child sub-scorers positioned on the current document
   * @lucene.experimental
   */
  public Collection<ChildScorable> getChildren() throws IOException {
    return Collections.emptyList();
  }

  /** A child Scorer and its relationship to its parent.
   * the meaning of the relationship depends upon the parent query.
   * @lucene.experimental */
  public static class ChildScorable {
    /**
     * Child Scorer. (note this is typically a direct child, and may
     * itself also have children).
     */
    public final Scorable child;
    /**
     * An arbitrary string relating this scorer to the parent.
     */
    public final String relationship;

    /**
     * Creates a new ChildScorer node with the specified relationship.
     * <p>
     * The relationship can be any be any string that makes sense to
     * the parent Scorer.
     */
    public ChildScorable(Scorable child, String relationship) {
      this.child = child;
      this.relationship = relationship;
    }
  }

}
