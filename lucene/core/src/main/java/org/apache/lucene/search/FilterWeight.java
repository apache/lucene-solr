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
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;

/**
 * A {@code FilterWeight} contains another {@code Weight} and implements
 * all abstract methods by calling the contained weight's method.
 *
 * Note that {@code FilterWeight} does not override the non-abstract
 * {@link Weight#bulkScorer(LeafReaderContext)} method and subclasses of
 * {@code FilterWeight} must provide their bulkScorer implementation
 * if required.
 *
 * @lucene.internal
 */
public abstract class FilterWeight extends Weight {

  final protected Weight in;

  /**
   * Default constructor.
   */
  protected FilterWeight(Weight weight) {
    this(weight.getQuery(), weight);
  }

  /**
   * Alternative constructor.
   * Use this variant only if the <code>weight</code> was not obtained
   * via the {@link Query#createWeight(IndexSearcher, ScoreMode, float)}
   * method of the <code>query</code> object.
   */
  protected FilterWeight(Query query, Weight weight) {
    super(query);
    this.in = weight;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return in.isCacheable(ctx);
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
  public Scorer scorer(LeafReaderContext context) throws IOException {
    return in.scorer(context);
  }

  @Override
  public Matches matches(LeafReaderContext context, int doc) throws IOException {
    return in.matches(context, doc);
  }
}
