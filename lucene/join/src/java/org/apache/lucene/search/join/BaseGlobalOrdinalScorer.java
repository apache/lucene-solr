package org.apache.lucene.search.join;

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

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.LongBitSet;

import java.io.IOException;

abstract class BaseGlobalOrdinalScorer extends Scorer {

  final SortedDocValues values;
  final Scorer approximationScorer;

  float score;

  public BaseGlobalOrdinalScorer(Weight weight, SortedDocValues values, Scorer approximationScorer) {
    super(weight);
    this.values = values;
    this.approximationScorer = approximationScorer;
  }

  @Override
  public float score() throws IOException {
    return score;
  }

  @Override
  public int docID() {
    return approximationScorer.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(approximationScorer.docID() + 1);
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    final DocIdSetIterator approximation = new DocIdSetIterator() {
      @Override
      public int docID() {
        return approximationScorer.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return approximationScorer.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return approximationScorer.advance(target);
      }

      @Override
      public long cost() {
        return approximationScorer.cost();
      }
    };
    return createTwoPhaseIterator(approximation);
  }

  @Override
  public long cost() {
    return approximationScorer.cost();
  }

  @Override
  public int freq() throws IOException {
    return 1;
  }

  protected abstract TwoPhaseIterator createTwoPhaseIterator(DocIdSetIterator approximation);

}