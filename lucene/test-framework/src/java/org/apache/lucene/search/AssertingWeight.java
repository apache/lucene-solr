package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.Bits;

class AssertingWeight extends Weight {

  static Weight wrap(Random random, Weight other) {
    return other instanceof AssertingWeight ? other : new AssertingWeight(random, other);
  }

  final Random random;
  final Weight in;

  AssertingWeight(Random random, Weight in) {
    this.random = random;
    this.in = in;
  }

  @Override
  public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
    return in.explain(context, doc);
  }

  @Override
  public Query getQuery() {
    return in.getQuery();
  }

  @Override
  public float getValueForNormalization() throws IOException {
    return in.getValueForNormalization();
  }

  @Override
  public void normalize(float norm, float topLevelBoost) {
    in.normalize(norm, topLevelBoost);
  }

  @Override
  public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    // if the caller asks for in-order scoring or if the weight does not support
    // out-of order scoring then collection will have to happen in-order.
    final Scorer inScorer = in.scorer(context, acceptDocs);
    return AssertingScorer.wrap(new Random(random.nextLong()), inScorer);
  }

  @Override
  public TopScorer topScorer(AtomicReaderContext context, boolean scoreDocsInOrder, Bits acceptDocs) throws IOException {
    // if the caller asks for in-order scoring or if the weight does not support
    // out-of order scoring then collection will have to happen in-order.
    TopScorer inScorer = in.topScorer(context, scoreDocsInOrder, acceptDocs);
    if (inScorer == null) {
      return null;
    }
    if (AssertingTopScorer.shouldWrap(inScorer)) {
      return AssertingTopScorer.wrap(new Random(random.nextLong()), inScorer);
    } else {
      // Let super wrap this.scorer instead, so we use
      // AssertingScorer:
      return super.topScorer(context, scoreDocsInOrder, acceptDocs);
    }
  }

  @Override
  public boolean scoresDocsOutOfOrder() {
    return in.scoresDocsOutOfOrder();
  }

}

