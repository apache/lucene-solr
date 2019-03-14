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
package org.apache.lucene.queries.function;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * A Query wrapping a {@link ValueSource} that matches docs in which the values in the value source match a configured
 * range.  The score is the float value.  This can be a slow query if run by itself since it must visit all docs;
 * ideally it's combined with other queries.
 * It's mostly a wrapper around
 * {@link FunctionValues#getRangeScorer(Weight, LeafReaderContext, String, String, boolean, boolean)}.
 *
 * A similar class is {@code org.apache.lucene.search.DocValuesRangeQuery} in the sandbox module.  That one is
 * constant scoring.
 *
 * @see FunctionQuery (constant scoring)
 * @lucene.experimental
 */
public class FunctionRangeQuery extends Query {

  private final ValueSource valueSource;

  // These two are declared as strings because FunctionValues.getRangeScorer takes String args and parses them.
  private final String lowerVal;
  private final String upperVal;
  private final boolean includeLower;
  private final boolean includeUpper;

  public FunctionRangeQuery(ValueSource valueSource, Number lowerVal, Number upperVal,
                            boolean includeLower, boolean includeUpper) {
    this(valueSource, lowerVal == null ? null : lowerVal.toString(), upperVal == null ? null : upperVal.toString(),
        includeLower, includeUpper);
  }

  public FunctionRangeQuery(ValueSource valueSource, String lowerVal, String upperVal,
                            boolean includeLower, boolean includeUpper) {
    this.valueSource = valueSource;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
  }

  public ValueSource getValueSource() {
    return valueSource;
  }

  public String getLowerVal() {
    return lowerVal;
  }

  public String getUpperVal() {
    return upperVal;
  }

  public boolean isIncludeLower() {
    return includeLower;
  }

  public boolean isIncludeUpper() {
    return includeUpper;
  }

  @Override
  public String toString(String field) {
    return "frange(" + valueSource + "):"
        + (includeLower ? '[' : '{')
        + (lowerVal == null ? "*" : lowerVal) + " TO " + (upperVal == null ? "*" : upperVal)
        + (includeUpper ? ']' : '}');
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(FunctionRangeQuery other) {
    return Objects.equals(includeLower, other.includeLower) &&
           Objects.equals(includeUpper, other.includeUpper) &&
           Objects.equals(valueSource, other.valueSource) &&
           Objects.equals(lowerVal, other.lowerVal) &&
           Objects.equals(upperVal, other.upperVal);
  }

  @Override
  public int hashCode() {
    return classHash() ^ Objects.hash(valueSource, lowerVal, upperVal, includeLower, includeUpper);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new FunctionRangeWeight(searcher);
  }

  private class FunctionRangeWeight extends Weight {
    @SuppressWarnings("rawtypes")
    private final Map vsContext;

    public FunctionRangeWeight(IndexSearcher searcher) throws IOException {
      super(FunctionRangeQuery.this);
      vsContext = ValueSource.newContext(searcher);
      valueSource.createWeight(vsContext, searcher);//callback on valueSource tree
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      //none
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      FunctionValues functionValues = valueSource.getValues(vsContext, context);
      //note: by using ValueSourceScorer directly, we avoid calling scorer.advance(doc) and checking if true,
      //  which can be slow since if that doc doesn't match, it has to linearly find the next matching
      ValueSourceScorer scorer = scorer(context);
      if (scorer.matches(doc)) {
        scorer.iterator().advance(doc);
        return Explanation.match(scorer.score(), FunctionRangeQuery.this.toString(), functionValues.explain(doc));
      } else {
        return Explanation.noMatch(FunctionRangeQuery.this.toString(), functionValues.explain(doc));
      }
    }

    @Override
    public ValueSourceScorer scorer(LeafReaderContext context) throws IOException {
      FunctionValues functionValues = valueSource.getValues(vsContext, context);
      // getRangeScorer takes String args and parses them. Weird.
      return functionValues.getRangeScorer(this, context, lowerVal, upperVal, includeLower, includeUpper);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }
}
