package org.apache.lucene.document;

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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** Sorts a field by a provided deferenced sort key. */
class SortKeyComparator extends SimpleFieldComparator<BytesRef> {

  // TODO: we could cache the sort keys...
  private final BytesRef[] values;
  private final BytesRefBuilder[] tempBRs;
  private BinaryDocValues docTerms;
  private Bits docsWithField;
  private final String field;
  private BytesRef bottom;
  private BytesRef topValue;
  private final int missingSortCmp;
  private final FieldTypes.SortKey sortKey;

  /** Sole constructor. */
  public SortKeyComparator(int numHits, String field, boolean sortMissingLast, FieldTypes.SortKey sortKey) {
    values = new BytesRef[numHits];
    tempBRs = new BytesRefBuilder[numHits];
    this.sortKey = sortKey;
    this.field = field;
    missingSortCmp = sortMissingLast ? 1 : -1;
  }

  @Override
  public int compare(int slot1, int slot2) {
    return compareValues(values[slot1], values[slot2]);
  }

  @Override
  public int compareBottom(int doc) {
    final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
    return compareValues(bottom, comparableBytes);
  }

  @Override
  public void copy(int slot, int doc) {
    final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
    if (comparableBytes == null) {
      values[slot] = null;
    } else {
      if (tempBRs[slot] == null) {
        tempBRs[slot] = new BytesRefBuilder();
      }
      tempBRs[slot].copyBytes(comparableBytes);
      values[slot] = tempBRs[slot].get();
    }
  }

  /** Check whether the given value represents <tt>null</tt>. This can be
   *  useful if the {@link BinaryDocValues} returned by {@link #getBinaryDocValues}
   *  use a special value as a sentinel. The default implementation checks
   *  {@link #getDocsWithField}.
   *  <p>NOTE: The null value can only be an EMPTY {@link BytesRef}. */
  protected boolean isNull(int doc, BytesRef term) {
    return docsWithField != null && docsWithField.get(doc) == false;
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    docTerms = DocValues.getBinary(context.reader(), field);
    docsWithField = DocValues.getDocsWithField(context.reader(), field);
    if (docsWithField instanceof Bits.MatchAllBits) {
      docsWithField = null;
    }
  }
    
  @Override
  public void setBottom(final int bottom) {
    this.bottom = values[bottom];
  }

  @Override
  public void setTopValue(BytesRef value) {
    // null is fine: it means the last doc of the prior
    // search was missing this value
    topValue = value;
  }

  @Override
  public BytesRef value(int slot) {
    return values[slot];
  }

  @Override
  public int compareValues(BytesRef val1, BytesRef val2) {
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return sortKey.getKey(val1).compareTo(sortKey.getKey(val2));
  }

  @Override
  public int compareTop(int doc) {
    final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
    return compareValues(topValue, comparableBytes);
  }

  /**
   * Given a document and a term, return the term itself if it exists or
   * <tt>null</tt> otherwise.
   */
  private BytesRef getComparableBytes(int doc, BytesRef term) {
    if (term.length == 0 && docsWithField != null && docsWithField.get(doc) == false) {
      return null;
    }
    return term;
  }
}
