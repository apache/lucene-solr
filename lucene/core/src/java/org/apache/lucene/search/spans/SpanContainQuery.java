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
package org.apache.lucene.search.spans;


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

abstract class SpanContainQuery extends SpanQuery implements Cloneable {

  SpanQuery big;
  SpanQuery little;

  SpanContainQuery(SpanQuery big, SpanQuery little) {
    this.big = Objects.requireNonNull(big);
    this.little = Objects.requireNonNull(little);
    Objects.requireNonNull(big.getField());
    Objects.requireNonNull(little.getField());
    if (! big.getField().equals(little.getField())) {
      throw new IllegalArgumentException("big and little not same field");
    }
  }

  @Override
  public String getField() { return big.getField(); }
  
  public SpanQuery getBig() {
    return big;
  }

  public SpanQuery getLittle() {
    return little;
  }

  public abstract class SpanContainWeight extends SpanWeight {

    final SpanWeight bigWeight;
    final SpanWeight littleWeight;

    public SpanContainWeight(IndexSearcher searcher, Map<Term, TermStates> terms,
                             SpanWeight bigWeight, SpanWeight littleWeight, float boost) throws IOException {
      super(SpanContainQuery.this, searcher, terms, boost);
      this.bigWeight = bigWeight;
      this.littleWeight = littleWeight;
    }

    /**
     * Extract terms from both <code>big</code> and <code>little</code>.
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      bigWeight.extractTerms(terms);
      littleWeight.extractTerms(terms);
    }

    ArrayList<Spans> prepareConjunction(final LeafReaderContext context, Postings postings) throws IOException {
      Spans bigSpans = bigWeight.getSpans(context, postings);
      if (bigSpans == null) {
        return null;
      }
      Spans littleSpans = littleWeight.getSpans(context, postings);
      if (littleSpans == null) {
        return null;
      }
      ArrayList<Spans> bigAndLittle = new ArrayList<>();
      bigAndLittle.add(bigSpans);
      bigAndLittle.add(littleSpans);
      return bigAndLittle;
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      bigWeight.extractTermStates(contexts);
      littleWeight.extractTermStates(contexts);
    }

  }

  String toString(String field, String name) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(name);
    buffer.append("(");
    buffer.append(big.toString(field));
    buffer.append(", ");
    buffer.append(little.toString(field));
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanQuery rewrittenBig = (SpanQuery) big.rewrite(reader);
    SpanQuery rewrittenLittle = (SpanQuery) little.rewrite(reader);
    if (big != rewrittenBig || little != rewrittenLittle) {
      try {
        SpanContainQuery clone = (SpanContainQuery) super.clone();
        clone.big = rewrittenBig;
        clone.little = rewrittenLittle;
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new AssertionError(e);
      }
    }
    return super.rewrite(reader);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  } 
  
  private boolean equalsTo(SpanContainQuery other) {
    return big.equals(other.big) && 
           little.equals(other.little);
  }

  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= big.hashCode();
    h = Integer.rotateLeft(h, 1);
    h ^= little.hashCode();
    return h;
  }
}