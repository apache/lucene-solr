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

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/** Provides a comparator that parses a feature field's values as float and sorts by descending value */
public class FeatureComparatorSource extends FieldComparatorSource {

  private final BytesRef featureName;
  private final Float missingValue;

  /**
   * Creates a {@link FieldComparatorSource} that can be used to sort hits by
   * the value of a particular feature in a {@link FeatureField}. Note that the
   * field name to use for the sort will be specified by the {@link SortField}.
   *
   * @param featureName The name of the feature to use for the sort value
   * @param missingValue The value to use for documents that don't have a value
   *   for the feature. If <code>null</code>, documents with no value for the
   *   feature will be sorted as if their value is <code>0.0f</code>.
   */
  public FeatureComparatorSource(BytesRef featureName, Float missingValue) {
    this.featureName = featureName;
    this.missingValue = missingValue;
  }

  @Override
  public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
    return new FeatureComparator(numHits, fieldname, featureName, missingValue);
  }

  /** Parses a feature field's values as float and sorts by descending value */
  class FeatureComparator extends SimpleFieldComparator<Float> {
    private final String field;
    private final BytesRef featureName;
    private final float missingValue;
    private final float[] values;
    private float bottom;
    private float topValue;
    private PostingsEnum currentReaderPostingsValues;

    /** Creates a new comparator based on relevance for {@code numHits}. */
    public FeatureComparator(int numHits, String field, BytesRef featureName, Float missingValue) {
      this.values = new float[numHits];
      this.field = field;
      this.featureName = featureName;
      this.missingValue = missingValue != null ? missingValue : 0.0f;
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
      if (currentReaderPostingsValues != null
          && (currentReaderPostingsValues.docID() == doc || currentReaderPostingsValues.advance(doc) == doc)) {
        return currentReaderPostingsValues.freq();
      } else {
        return missingValue;
      }
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Float.compare(values[slot2], values[slot1]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Float.compare(getValueForDoc(doc), bottom);
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
      return Float.compare(getValueForDoc(doc), topValue);
    }
  }
}