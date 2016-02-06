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
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;

class AssertingWeight extends Weight {

  final Random random;
  final Weight in;
  final boolean needsScores;

  AssertingWeight(Random random, Weight in, boolean needsScores) {
    super(in.getQuery());
    this.random = random;
    this.in = in;
    this.needsScores = needsScores;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    in.extractTerms(terms);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    return in.explain(context, doc);
  }

  @Override
  public float getValueForNormalization() throws IOException {
    return in.getValueForNormalization();
  }

  @Override
  public void normalize(float norm, float boost) {
    in.normalize(norm, boost);
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    final Scorer inScorer = in.scorer(context);
    assert inScorer == null || inScorer.docID() == -1;
    return AssertingScorer.wrap(new Random(random.nextLong()), inScorer, needsScores);
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    BulkScorer inScorer = in.bulkScorer(context);
    if (inScorer == null) {
      return null;
    }

    return AssertingBulkScorer.wrap(new Random(random.nextLong()), inScorer, context.reader().maxDoc());
  }
}
