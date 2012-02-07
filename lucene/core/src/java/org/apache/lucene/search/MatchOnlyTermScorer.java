package org.apache.lucene.search;

/**
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

import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.similarities.Similarity;

/** Expert: A <code>Scorer</code> for documents matching a
 * <code>Term</code>.  It treats all documents as having
 * one occurrenc (tf=1) for the term.
 */

final class MatchOnlyTermScorer extends Scorer {
  private final DocsEnum docsEnum;
  private final Similarity.ExactSimScorer docScorer;
  
  /**
   * Construct a <code>TermScorer</code>.
   * 
   * @param weight
   *          The weight of the <code>Term</code> in the query.
   * @param td
   *          An iterator over the documents matching the <code>Term</code>.
   * @param docScorer
   *          The </code>Similarity.ExactSimScorer</code> implementation 
   *          to be used for score computations.
   */
  MatchOnlyTermScorer(Weight weight, DocsEnum td, Similarity.ExactSimScorer docScorer) throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.docsEnum = td;
  }

  @Override
  public int docID() {
    return docsEnum.docID();
  }

  @Override
  public float freq() {
    return 1.0f;
  }

  /**
   * Advances to the next document matching the query. <br>
   * 
   * @return the document matching the query or NO_MORE_DOCS if there are no more documents.
   */
  @Override
  public int nextDoc() throws IOException {
    return docsEnum.nextDoc();
  }

  @Override
  public float score() {
    assert docID() != NO_MORE_DOCS;
    return docScorer.score(docsEnum.docID(), 1);
  }

  /**
   * Advances to the first match beyond the current whose document number is
   * greater than or equal to a given target. <br>
   * The implementation uses {@link DocsEnum#advance(int)}.
   * 
   * @param target
   *          The target document number.
   * @return the matching document or NO_MORE_DOCS if none exist.
   */
  @Override
  public int advance(int target) throws IOException {
    return docsEnum.advance(target);
  }

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() { return "scorer(" + weight + ")"; }
}
