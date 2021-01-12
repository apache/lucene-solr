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
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;

/**
 * Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 *
 * @lucene.internal
 */
public final class TermScorer extends Scorer {
  private final PostingsEnum postingsEnum;
  private final ImpactsEnum impactsEnum;
  private final DocIdSetIterator iterator;
  private final LeafSimScorer docScorer;
  private final ImpactsDISI impactsDisi;

  /** Construct a {@link TermScorer} that will iterate all documents. */
  public TermScorer(Weight weight, PostingsEnum postingsEnum, LeafSimScorer docScorer) {
    super(weight);
    iterator = this.postingsEnum = postingsEnum;
    impactsEnum = new SlowImpactsEnum(postingsEnum);
    impactsDisi = new ImpactsDISI(impactsEnum, impactsEnum, docScorer.getSimScorer());
    this.docScorer = docScorer;
  }

  /**
   * Construct a {@link TermScorer} that will use impacts to skip blocks of non-competitive
   * documents.
   */
  TermScorer(Weight weight, ImpactsEnum impactsEnum, LeafSimScorer docScorer) {
    super(weight);
    postingsEnum = this.impactsEnum = impactsEnum;
    impactsDisi = new ImpactsDISI(impactsEnum, impactsEnum, docScorer.getSimScorer());
    iterator = impactsDisi;
    this.docScorer = docScorer;
  }

  @Override
  public int docID() {
    return postingsEnum.docID();
  }

  /** Returns term frequency in the current document. */
  public final int freq() throws IOException {
    return postingsEnum.freq();
  }

  @Override
  public DocIdSetIterator iterator() {
    return iterator;
  }

  @Override
  public float score() throws IOException {
    assert docID() != DocIdSetIterator.NO_MORE_DOCS;
    return docScorer.score(postingsEnum.docID(), postingsEnum.freq());
  }

  @Override
  public float smoothingScore(int docId) throws IOException {
    return docScorer.score(docId, 0);
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return impactsDisi.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return impactsDisi.getMaxScore(upTo);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    impactsDisi.setMinCompetitiveScore(minScore);
  }

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() {
    return "scorer(" + weight + ")[" + super.toString() + "]";
  }
}
