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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.document.FeatureField.FeatureDoubleValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/**
 * Sorts using the value of a specified feature name from a {@link FeatureField}.
 */
final class FeatureSortField extends SortField {

  private final String featureName;

  /**
   * Creates a {@link FeatureSortField} that can be used to sort hits by
   * the value of a particular feature in a {@link FeatureField}.
   *
   * @param featureName The name of the feature to use for the sort value
   */
  public FeatureSortField(String field, String featureName) {
    super(Objects.requireNonNull(field), SortField.Type.CUSTOM);
    this.featureName = Objects.requireNonNull(featureName);
  }
  
  @Override
  public FieldComparator<?> getComparator(int numHits, int sortPos) {
    return new FeatureComparator(numHits, getField(), featureName);
  }
  
  @Override
  public void setMissingValue(Object missingValue) {
    throw new IllegalArgumentException("Missing value not supported for FeatureSortField");
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + featureName.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    FeatureSortField other = (FeatureSortField) obj;
    return Objects.equals(featureName, other.featureName);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("<feature:");
    builder.append('"');
    builder.append(getField());
    builder.append('"');
    builder.append(" featureName=");
    builder.append(featureName);
    builder.append('>');
    return builder.toString();
  }

  /** Parses a feature field's values as float and sorts by descending value */
  class FeatureComparator extends SimpleFieldComparator<Double> {
    private final String field;
    private final BytesRef featureName;
    private final double[] values;
    private double bottom;
    private double topValue;
    private FeatureDoubleValues featureValues;

    /** Creates a new comparator based on relevance for {@code numHits}. */
    public FeatureComparator(int numHits, String field, String featureName) {
      this.values = new double[numHits];
      this.field = field;
      this.featureName = new BytesRef(featureName);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      featureValues = new FeatureDoubleValues(context, field, featureName);
    }

    private double getValueForDoc(int doc) throws IOException {
      if (featureValues.advanceExact(doc)) {
        return featureValues.doubleValue();
      } else {
        return 0.0;
      }
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Double.compare(values[slot2], values[slot1]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Double.compare(getValueForDoc(doc), bottom);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values[slot] = getValueForDoc(doc);
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Double value) {
      topValue = value;
    }

    @Override
    public Double value(int slot) {
      return Double.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Double.compare(getValueForDoc(doc), topValue);
    }
  }
}
