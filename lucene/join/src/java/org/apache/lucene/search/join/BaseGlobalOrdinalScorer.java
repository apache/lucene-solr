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
package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

abstract class BaseGlobalOrdinalScorer extends Scorer {

  final SortedDocValues values;
  final DocIdSetIterator approximation;

  float score;

  public BaseGlobalOrdinalScorer(Weight weight, SortedDocValues values, DocIdSetIterator approximationScorer) {
    super(weight);
    this.values = values;
    this.approximation = approximationScorer;
  }

  @Override
  public float score() throws IOException {
    return score;
  }

  @Override
  public int docID() {
    return approximation.docID();
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return createTwoPhaseIterator(approximation);
  }

  protected abstract TwoPhaseIterator createTwoPhaseIterator(DocIdSetIterator approximation);

}
