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
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;

/**
 * A Weight that has a constant score equal to the boost of the wrapped query.
 *
 * @lucene.internal
 */
public abstract class ConstantScoreWeight extends Weight {

  private float queryNorm;
  private float queryWeight;

  protected ConstantScoreWeight(Query query) {
    super(query);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    // most constant-score queries don't wrap index terms
    // eg. geo filters, doc values queries, ...
    // override if your constant-score query does wrap terms
  }

  @Override
  public final float getValueForNormalization() throws IOException {
    queryWeight = getQuery().getBoost();
    return queryWeight * queryWeight;
  }

  @Override
  public final void normalize(float norm, float topLevelBoost) {
    queryNorm = norm * topLevelBoost;
    queryWeight *= queryNorm;
  }

  @Override
  public final Explanation explain(LeafReaderContext context, int doc) throws IOException {
    final Scorer s = scorer(context, context.reader().getLiveDocs());
    final boolean exists = (s != null && s.advance(doc) == doc);

    if (exists) {
      return Explanation.match(
          queryWeight, getQuery().toString() + ", product of:",
          Explanation.match(getQuery().getBoost(), "boost"), Explanation.match(queryNorm, "queryNorm"));
    } else {
      return Explanation.noMatch(getQuery().toString() + " doesn't match id " + doc);
    }
  }

  @Override
  public final Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
    return scorer(context, acceptDocs, queryWeight);
  }

  protected abstract Scorer scorer(LeafReaderContext context, Bits acceptDocs, float score) throws IOException;

}
