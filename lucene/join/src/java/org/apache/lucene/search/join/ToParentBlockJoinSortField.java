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

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.NumericUtils;

/**
 * A special sort field that allows sorting parent docs based on nested / child level fields.
 * Based on the sort order it either takes the document with the lowest or highest field value into account.
 *
 * @lucene.experimental
 */
public class ToParentBlockJoinSortField extends SortField {

  private final boolean order;
  private final BitSetProducer parentFilter;
  private final BitSetProducer childFilter;

  /**
   * Create ToParentBlockJoinSortField. The parent document ordering is based on child document ordering (reverse).
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverse Whether natural order should be reversed on the nested / child level.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(String field, Type type, boolean reverse, BitSetProducer parentFilter, BitSetProducer childFilter) {
    super(field, type, reverse);
    switch (getType()) {
      case STRING:
      case DOUBLE:
      case FLOAT:
      case LONG:
      case INT:
        // ok
        break;
      default:
        throw new UnsupportedOperationException("Sort type " + type + " is not supported");
    }
    this.order = reverse;
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
  }

  /**
   * Create ToParentBlockJoinSortField.
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverse Whether natural order should be reversed on the nested / child document level.
   * @param order Whether natural order should be reversed on the parent level.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(String field, Type type, boolean reverse, boolean order, BitSetProducer parentFilter, BitSetProducer childFilter) {
    super(field, type, reverse);
    this.order = order;
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, int sortPos) {
    switch (getType()) {
      case STRING:
        return getStringComparator(numHits);
      case DOUBLE:
        return getDoubleComparator(numHits);
      case FLOAT:
        return getFloatComparator(numHits);
      case LONG:
        return getLongComparator(numHits);
      case INT:
        return getIntComparator(numHits);
      default:
        throw new UnsupportedOperationException("Sort type " + getType() + " is not supported");
    }
  }

  private FieldComparator<?> getStringComparator(int numHits) {
    return new FieldComparator.TermOrdValComparator(numHits, getField(), missingValue == STRING_LAST) {

      @Override
      protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
        SortedSetDocValues sortedSet = DocValues.getSortedSet(context.reader(), field);
        final BlockJoinSelector.Type type = order
            ? BlockJoinSelector.Type.MAX
            : BlockJoinSelector.Type.MIN;
        final BitSet parents = parentFilter.getBitSet(context);
        final BitSet children = childFilter.getBitSet(context);
        if (children == null) {
          return DocValues.emptySorted();
        }
        return BlockJoinSelector.wrap(sortedSet, type, parents, children);
      }

    };
  }

  private FieldComparator<?> getIntComparator(int numHits) {
    return new FieldComparator.IntComparator(numHits, getField(), (Integer) missingValue) {
      @Override
      protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
        SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(context.reader(), field);
        final BlockJoinSelector.Type type = order
            ? BlockJoinSelector.Type.MAX
            : BlockJoinSelector.Type.MIN;
        final BitSet parents = parentFilter.getBitSet(context);
        final BitSet children = childFilter.getBitSet(context);
        if (children == null) {
          return DocValues.emptyNumeric();
        }
        return BlockJoinSelector.wrap(sortedNumeric, type, parents, children);
      }
    };
  }

  private FieldComparator<?> getLongComparator(int numHits) {
    return new FieldComparator.LongComparator(numHits, getField(), (Long) missingValue) {
      @Override
      protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
        SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(context.reader(), field);
        final BlockJoinSelector.Type type = order
            ? BlockJoinSelector.Type.MAX
            : BlockJoinSelector.Type.MIN;
        final BitSet parents = parentFilter.getBitSet(context);
        final BitSet children = childFilter.getBitSet(context);
        if (children == null) {
          return DocValues.emptyNumeric();
        }
        return BlockJoinSelector.wrap(sortedNumeric, type, parents, children);
      }
    };
  }

  private FieldComparator<?> getFloatComparator(int numHits) {
    return new FieldComparator.FloatComparator(numHits, getField(), (Float) missingValue) {
      @Override
      protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
        SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(context.reader(), field);
        final BlockJoinSelector.Type type = order
            ? BlockJoinSelector.Type.MAX
            : BlockJoinSelector.Type.MIN;
        final BitSet parents = parentFilter.getBitSet(context);
        final BitSet children = childFilter.getBitSet(context);
        if (children == null) {
          return DocValues.emptyNumeric();
        }
        return new FilterNumericDocValues(BlockJoinSelector.wrap(sortedNumeric, type, parents, children)) {
          @Override
          public long longValue() throws IOException {
            // undo the numericutils sortability
            return NumericUtils.sortableFloatBits((int) super.longValue());
          }
        };
      }
    };
  }

  private FieldComparator<?> getDoubleComparator(int numHits) {
    return new FieldComparator.DoubleComparator(numHits, getField(), (Double) missingValue) {
      @Override
      protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
        SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(context.reader(), field);
        final BlockJoinSelector.Type type = order
            ? BlockJoinSelector.Type.MAX
            : BlockJoinSelector.Type.MIN;
        final BitSet parents = parentFilter.getBitSet(context);
        final BitSet children = childFilter.getBitSet(context);
        if (children == null) {
          return DocValues.emptyNumeric();
        }
        return new FilterNumericDocValues(BlockJoinSelector.wrap(sortedNumeric, type, parents, children)) {
          @Override
          public long longValue() throws IOException {
            // undo the numericutils sortability
            return NumericUtils.sortableDoubleBits(super.longValue());
          }
        };
      }
    };
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((childFilter == null) ? 0 : childFilter.hashCode());
    result = prime * result + (order ? 1231 : 1237);
    result = prime * result + ((parentFilter == null) ? 0 : parentFilter.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    ToParentBlockJoinSortField other = (ToParentBlockJoinSortField) obj;
    if (childFilter == null) {
      if (other.childFilter != null) return false;
    } else if (!childFilter.equals(other.childFilter)) return false;
    if (order != other.order) return false;
    if (parentFilter == null) {
      if (other.parentFilter != null) return false;
    } else if (!parentFilter.equals(other.parentFilter)) return false;
    return true;
  }
}
