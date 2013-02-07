package org.apache.lucene.sandbox.queries;

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
import java.text.Collator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.BytesRef;

/** Sorts by a field's value using the given Collator
 *
 * <p><b>WARNING</b>: this is very slow; you'll
 * get much better performance using the
 * CollationKeyAnalyzer or ICUCollationKeyAnalyzer. 
 * @deprecated Index collation keys with CollationKeyAnalyzer or ICUCollationKeyAnalyzer instead.
 * This class will be removed in Lucene 5.0
 */
@Deprecated
public final class SlowCollatedStringComparator extends FieldComparator<String> {

  private final String[] values;
  private BinaryDocValues currentDocTerms;
  private final String field;
  final Collator collator;
  private String bottom;
  private final BytesRef tempBR = new BytesRef();

  public SlowCollatedStringComparator(int numHits, String field, Collator collator) {
    values = new String[numHits];
    this.field = field;
    this.collator = collator;
  }

  @Override
  public int compare(int slot1, int slot2) {
    final String val1 = values[slot1];
    final String val2 = values[slot2];
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return -1;
    } else if (val2 == null) {
      return 1;
    }
    return collator.compare(val1, val2);
  }

  @Override
  public int compareBottom(int doc) {
    currentDocTerms.get(doc, tempBR);
    final String val2 = tempBR.bytes == BinaryDocValues.MISSING ? null : tempBR.utf8ToString();
    if (bottom == null) {
      if (val2 == null) {
        return 0;
      }
      return -1;
    } else if (val2 == null) {
      return 1;
    }
    return collator.compare(bottom, val2);
  }

  @Override
  public void copy(int slot, int doc) {
    currentDocTerms.get(doc, tempBR);
    if (tempBR.bytes == BinaryDocValues.MISSING) {
      values[slot] = null;
    } else {
      values[slot] = tempBR.utf8ToString();
    }
  }

  @Override
  public FieldComparator<String> setNextReader(AtomicReaderContext context) throws IOException {
    currentDocTerms = FieldCache.DEFAULT.getTerms(context.reader(), field);
    return this;
  }
  
  @Override
  public void setBottom(final int bottom) {
    this.bottom = values[bottom];
  }

  @Override
  public String value(int slot) {
    return values[slot];
  }

  @Override
  public int compareValues(String first, String second) {
    if (first == null) {
      if (second == null) {
        return 0;
      }
      return -1;
    } else if (second == null) {
      return 1;
    } else {
      return collator.compare(first, second);
    }
  }

  @Override
  public int compareDocToValue(int doc, String value) {
    currentDocTerms.get(doc, tempBR);
    final String docValue;
    if (tempBR.bytes == BinaryDocValues.MISSING) {
      docValue = null;
    } else {
      docValue = tempBR.utf8ToString();
    }
    return compareValues(docValue, value);
  }
}
