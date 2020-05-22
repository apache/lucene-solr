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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
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
    super(Objects.requireNonNull(field), SortField.Type.CUSTOM, true);
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
  class FeatureComparator extends SimpleFieldComparator<Float> {
    private final String field;
    private final BytesRef featureName;
    private final float[] values;
    private float bottom;
    private float topValue;
    private PostingsEnum currentReaderPostingsValues;

    /** Creates a new comparator based on relevance for {@code numHits}. */
    public FeatureComparator(int numHits, String field, String featureName) {
      this.values = new float[numHits];
      this.field = field;
      this.featureName = new BytesRef(featureName);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      Terms terms = context.reader().terms(field);
      if (terms == null) {
        currentReaderPostingsValues = null;
      } else {
        TermsEnum termsEnum = terms.iterator();
        if (termsEnum.seekExact(featureName) == false) {
          currentReaderPostingsValues = null;
        } else {
          currentReaderPostingsValues = termsEnum.postings(currentReaderPostingsValues, PostingsEnum.FREQS);
        }
      }
    }

    private float getValueForDoc(int doc) throws IOException {
      if (currentReaderPostingsValues != null && doc >= currentReaderPostingsValues.docID()
          && (currentReaderPostingsValues.docID() == doc || currentReaderPostingsValues.advance(doc) == doc)) {
        return FeatureField.decodeFeatureValue(currentReaderPostingsValues.freq());
      } else {
        return 0.0f;
      }
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Float.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Float.compare(bottom, getValueForDoc(doc));
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
    public void setTopValue(Float value) {
      topValue = value;
    }

    @Override
    public Float value(int slot) {
      return Float.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Float.compare(topValue, getValueForDoc(doc));
    }
  }
}
