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
package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Like {@link DocValuesTermsQuery}, but this query only runs on a long {@link
 * NumericDocValuesField} or a {@link SortedNumericDocValuesField}, matching all documents whose
 * value in the specified field is contained in the provided set of long values.
 *
 * <p><b>NOTE</b>: be very careful using this query: it is typically much slower than using {@code
 * TermsQuery}, but in certain specialized cases may be faster.
 *
 * @lucene.experimental
 */
public class DocValuesNumbersQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(DocValuesNumbersQuery.class);

  private final String field;
  private final LongHashSet numbers;

  public DocValuesNumbersQuery(String field, long[] numbers) {
    this.field = Objects.requireNonNull(field);
    this.numbers = new LongHashSet(numbers);
  }

  public DocValuesNumbersQuery(String field, Collection<Long> numbers) {
    this.field = Objects.requireNonNull(field);
    this.numbers = new LongHashSet(numbers.stream().mapToLong(Long::longValue).toArray());
  }

  public DocValuesNumbersQuery(String field, Long... numbers) {
    this(field, new HashSet<Long>(Arrays.asList(numbers)));
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(DocValuesNumbersQuery other) {
    return field.equals(other.field) && numbers.equals(other.numbers);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + Objects.hash(field, numbers);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  public String getField() {
    return field;
  }

  public Set<Long> getNumbers() {
    return numbers;
  }

  @Override
  public String toString(String defaultField) {
    return new StringBuilder().append(field).append(": ").append(numbers.toString()).toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES
        + RamUsageEstimator.sizeOfObject(field)
        + RamUsageEstimator.sizeOfObject(numbers);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
        return new ConstantScoreScorer(
            this,
            score(),
            scoreMode,
            new TwoPhaseIterator(values) {

              @Override
              public boolean matches() throws IOException {
                int count = values.docValueCount();
                for (int i = 0; i < count; i++) {
                  if (numbers.contains(values.nextValue())) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public float matchCost() {
                return 5; // lookup in the set
              }
            });
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }
}
