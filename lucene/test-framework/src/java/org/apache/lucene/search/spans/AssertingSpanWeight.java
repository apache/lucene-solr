package org.apache.lucene.search.spans;

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
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Wraps a SpanWeight with additional asserts
 */
public class AssertingSpanWeight extends SpanWeight {

  final SpanWeight in;

  /**
   * Create an AssertingSpanWeight
   * @param in the SpanWeight to wrap
   * @throws IOException on error
   */
  public AssertingSpanWeight(IndexSearcher searcher, SpanWeight in) throws IOException {
    super((SpanQuery) in.getQuery(), searcher, null);
    this.in = in;
  }

  @Override
  public void extractTermContexts(Map<Term, TermContext> contexts) {
    in.extractTermContexts(contexts);
  }

  @Override
  public Spans getSpans(LeafReaderContext context, Postings requiredPostings) throws IOException {
    Spans spans = in.getSpans(context, requiredPostings);
    if (spans == null)
      return null;
    return new AssertingSpans(spans);
  }

  @Override
  public Similarity.SimScorer getSimScorer(LeafReaderContext context) throws IOException {
    return in.getSimScorer(context);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    in.extractTerms(terms);
  }

  @Override
  public float getValueForNormalization() throws IOException {
    return in.getValueForNormalization();
  }

  @Override
  public void normalize(float queryNorm, float boost) {
    in.normalize(queryNorm, boost);
  }

  @Override
  public SpanScorer scorer(LeafReaderContext context) throws IOException {
    return in.scorer(context);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    return in.explain(context, doc);
  }
}
