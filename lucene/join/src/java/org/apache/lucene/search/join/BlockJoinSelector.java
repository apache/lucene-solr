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
package org.apache.lucene.search.join;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Select a value from a block of documents.
 *  @lucene.internal */
public class BlockJoinSelector {

  private BlockJoinSelector() {}

  /** Type of selection to perform. If none of the documents in the block have
   *  a value then no value will be selected. */
  public enum Type {
    /** Only consider the minimum value from the block when sorting. */
    MIN,
    /** Only consider the maximum value from the block when sorting. */
    MAX;
  }

  /** Return a {@link Bits} instance that returns true if, and only if, any of
   *  the children of the given parent document has a value. */
  public static Bits wrap(final Bits docsWithValue, BitSet parents, BitSet children) {
    return new Bits() {

      @Override
      public boolean get(int docID) {
        assert parents.get(docID) : "this selector may only be used on parent documents";

        if (docID == 0) {
          // no children
          return false;
        }

        final int firstChild = parents.prevSetBit(docID - 1) + 1;
        for (int child = children.nextSetBit(firstChild); child < docID; child = children.nextSetBit(child + 1)) {
          if (docsWithValue.get(child)) {
            return true;
          }
        }
        return false;
      }

      @Override
      public int length() {
        return docsWithValue.length();
      }

    };
  }

  /** Wraps the provided {@link SortedSetDocValues} in order to only select
   *  one value per parent among its {@code children} using the configured
   *  {@code selection} type. */
  public static SortedDocValues wrap(SortedSetDocValues sortedSet, Type selection, BitSet parents, BitSet children) {
    SortedDocValues values;
    switch (selection) {
      case MIN:
        values = SortedSetSelector.wrap(sortedSet, SortedSetSelector.Type.MIN);
        break;
      case MAX:
        values = SortedSetSelector.wrap(sortedSet, SortedSetSelector.Type.MAX);
        break;
      default:
        throw new AssertionError();
    }
    return wrap(values, selection, parents, children);
  }

  /** Wraps the provided {@link SortedDocValues} in order to only select
   *  one value per parent among its {@code children} using the configured
   *  {@code selection} type. */
  public static SortedDocValues wrap(final SortedDocValues values, Type selection, BitSet parents, BitSet children) {
    return new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        assert parents.get(docID) : "this selector may only be used on parent documents";

        if (docID == 0) {
          // no children
          return -1;
        }

        final int firstChild = parents.prevSetBit(docID - 1) + 1;

        int ord = -1;
        for (int child = children.nextSetBit(firstChild); child < docID; child = children.nextSetBit(child + 1)) {
          final int childOrd = values.getOrd(child);
          switch (selection) {
            case MIN:
              if (ord == -1) {
                ord = childOrd;
              } else if (childOrd != -1) {
                ord = Math.min(ord, childOrd);
              }
              break;
            case MAX:
              ord = Math.max(ord, childOrd);
              break;
            default:
              throw new AssertionError();
          }
        }
        return ord;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return values.lookupOrd(ord);
      }

      @Override
      public int getValueCount() {
        return values.getValueCount();
      }

    };
  }

  /** Wraps the provided {@link SortedNumericDocValues} in order to only select
   *  one value per parent among its {@code children} using the configured
   *  {@code selection} type. */
  public static NumericDocValues wrap(SortedNumericDocValues sortedNumerics, Type selection, BitSet parents, BitSet children) {
    NumericDocValues values;
    switch (selection) {
      case MIN:
        values = SortedNumericSelector.wrap(sortedNumerics, SortedNumericSelector.Type.MIN, SortField.Type.LONG);
        break;
      case MAX:
        values = SortedNumericSelector.wrap(sortedNumerics, SortedNumericSelector.Type.MAX, SortField.Type.LONG);
        break;
      default:
        throw new AssertionError();
    }
    return wrap(values, DocValues.docsWithValue(sortedNumerics, parents.length()), selection, parents, children);
  }

  /** Wraps the provided {@link NumericDocValues} in order to only select
   *  one value per parent among its {@code children} using the configured
   *  {@code selection} type. */
  public static NumericDocValues wrap(final NumericDocValues values, Bits docsWithValue, Type selection, BitSet parents, BitSet children) {
    return new NumericDocValues() {

      @Override
      public long get(int docID) {
        assert parents.get(docID) : "this selector may only be used on parent documents";

        if (docID == 0) {
          // no children
          return 0;
        }

        final int firstChild = parents.prevSetBit(docID - 1) + 1;

        long value = 0;
        boolean hasValue = false;

        for (int child = children.nextSetBit(firstChild); child < docID; child = children.nextSetBit(child + 1)) {
          final long childValue = values.get(child);
          final boolean childHasValue = value != 0 || docsWithValue.get(child);

          if (hasValue == false) {
            value = childValue;
            hasValue = childHasValue;
          } else if (childHasValue) {
            switch (selection) {
              case MIN:
                value = Math.min(value, childValue);
                break;
              case MAX:
                value = Math.max(value, childValue);
                break;
              default:
                throw new AssertionError();
            }
          }
        }

        return value;
      }

    };
  }

}
