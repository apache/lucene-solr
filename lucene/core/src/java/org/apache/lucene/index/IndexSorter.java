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

package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Handles how documents should be sorted in an index, both within a segment and between segments.
 *
 * <p>Implementers must provide the following methods: {@link #getDocComparator(LeafReader,int)} -
 * an object that determines how documents within a segment are to be sorted {@link
 * #getComparableProviders(List)} - an array of objects that return a sortable long value per
 * document and segment {@link #getProviderName()} - the SPI-registered name of a {@link
 * SortFieldProvider} to serialize the sort
 *
 * <p>The companion {@link SortFieldProvider} should be registered with SPI via {@code
 * META-INF/services}
 */
public interface IndexSorter {

  /** Used for sorting documents across segments */
  interface ComparableProvider {
    /**
     * Returns a long so that the natural ordering of long values matches the ordering of doc IDs
     * for the given comparator
     */
    long getAsComparableLong(int docID) throws IOException;
  }

  /** A comparator of doc IDs, used for sorting documents within a segment */
  interface DocComparator {
    /**
     * Compare docID1 against docID2. The contract for the return value is the same as {@link
     * Comparator#compare(Object, Object)}.
     */
    int compare(int docID1, int docID2);
  }

  /**
   * Get an array of {@link ComparableProvider}, one per segment, for merge sorting documents in
   * different segments
   *
   * @param readers the readers to be merged
   */
  ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
      throws IOException;

  /**
   * Get a comparator that determines the sort order of docs within a single Reader.
   *
   * <p>NB We cannot simply use the {@link FieldComparator} API because it requires docIDs to be
   * sent in-order. The default implementations allocate array[maxDoc] to hold native values for
   * comparison, but 1) they are transient (only alive while sorting this one segment) and 2) in the
   * typical index sorting case, they are only used to sort newly flushed segments, which will be
   * smaller than merged segments
   *
   * @param reader the Reader to sort
   * @param maxDoc the number of documents in the Reader
   */
  DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException;

  /**
   * The SPI-registered name of a {@link SortFieldProvider} that will deserialize the parent
   * SortField
   */
  String getProviderName();

  /** Provide a NumericDocValues instance for a LeafReader */
  interface NumericDocValuesProvider {
    /** Returns the NumericDocValues instance for this LeafReader */
    NumericDocValues get(LeafReader reader) throws IOException;
  }

  /** Provide a SortedDocValues instance for a LeafReader */
  interface SortedDocValuesProvider {
    /** Returns the SortedDocValues instance for this LeafReader */
    SortedDocValues get(LeafReader reader) throws IOException;
  }

  /** Sorts documents based on integer values from a NumericDocValues instance */
  final class IntSorter implements IndexSorter {

    private final Integer missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;
    private final String providerName;

    /** Creates a new IntSorter */
    public IntSorter(
        String providerName,
        Integer missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
      this.providerName = providerName;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValue;
      if (this.missingValue != null) {
        missingValue = this.missingValue;
      } else {
        missingValue = 0L;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              if (values.advanceExact(docID)) {
                return values.longValue();
              } else {
                return missingValue;
              }
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      int[] values = new int[maxDoc];
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = (int) dvs.longValue();
      }

      return (docID1, docID2) -> reverseMul * Integer.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on long values from a NumericDocValues instance */
  final class LongSorter implements IndexSorter {

    private final String providerName;
    private final Long missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /** Creates a new LongSorter */
    public LongSorter(
        String providerName,
        Long missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValue;
      if (this.missingValue != null) {
        missingValue = this.missingValue;
      } else {
        missingValue = 0L;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              if (values.advanceExact(docID)) {
                return values.longValue();
              } else {
                return missingValue;
              }
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      long[] values = new long[maxDoc];
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = dvs.longValue();
      }

      return (docID1, docID2) -> reverseMul * Long.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on float values from a NumericDocValues instance */
  final class FloatSorter implements IndexSorter {

    private final String providerName;
    private final Float missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /** Creates a new FloatSorter */
    public FloatSorter(
        String providerName,
        Float missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final float missingValue;
      if (this.missingValue != null) {
        missingValue = this.missingValue;
      } else {
        missingValue = 0.0f;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              float value = missingValue;
              if (values.advanceExact(docID)) {
                value = Float.intBitsToFloat((int) values.longValue());
              }
              return NumericUtils.floatToSortableInt(value);
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      float[] values = new float[maxDoc];
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = Float.intBitsToFloat((int) dvs.longValue());
      }

      return (docID1, docID2) -> reverseMul * Float.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on double values from a NumericDocValues instance */
  final class DoubleSorter implements IndexSorter {

    private final String providerName;
    private final Double missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /** Creates a new DoubleSorter */
    public DoubleSorter(
        String providerName,
        Double missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final double missingValue;
      if (this.missingValue != null) {
        missingValue = this.missingValue;
      } else {
        missingValue = 0.0f;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              double value = missingValue;
              if (values.advanceExact(docID)) {
                value = Double.longBitsToDouble(values.longValue());
              }
              return NumericUtils.doubleToSortableLong(value);
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      double[] values = new double[maxDoc];
      if (missingValue != null) {
        Arrays.fill(values, missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = Double.longBitsToDouble(dvs.longValue());
      }

      return (docID1, docID2) -> reverseMul * Double.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on terms from a SortedDocValues instance */
  final class StringSorter implements IndexSorter {

    private final String providerName;
    private final Object missingValue;
    private final int reverseMul;
    private final SortedDocValuesProvider valuesProvider;

    /** Creates a new StringSorter */
    public StringSorter(
        String providerName,
        Object missingValue,
        boolean reverse,
        SortedDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      final ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final SortedDocValues[] values = new SortedDocValues[readers.size()];
      for (int i = 0; i < readers.size(); i++) {
        final SortedDocValues sorted = valuesProvider.get(readers.get(i));
        values[i] = sorted;
      }
      OrdinalMap ordinalMap = OrdinalMap.build(null, values, PackedInts.DEFAULT);
      final int missingOrd;
      if (missingValue == SortField.STRING_LAST) {
        missingOrd = Integer.MAX_VALUE;
      } else {
        missingOrd = Integer.MIN_VALUE;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final SortedDocValues readerValues = values[readerIndex];
        final LongValues globalOrds = ordinalMap.getGlobalOrds(readerIndex);
        providers[readerIndex] =
            docID -> {
              if (readerValues.advanceExact(docID)) {
                // translate segment's ord to global ord space:
                return globalOrds.get(readerValues.ordValue());
              } else {
                return missingOrd;
              }
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final SortedDocValues sorted = valuesProvider.get(reader);
      final int missingOrd;
      if (missingValue == SortField.STRING_LAST) {
        missingOrd = Integer.MAX_VALUE;
      } else {
        missingOrd = Integer.MIN_VALUE;
      }

      final int[] ords = new int[maxDoc];
      Arrays.fill(ords, missingOrd);
      int docID;
      while ((docID = sorted.nextDoc()) != NO_MORE_DOCS) {
        ords[docID] = sorted.ordValue();
      }

      return (docID1, docID2) -> reverseMul * Integer.compare(ords[docID1], ords[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }
}
