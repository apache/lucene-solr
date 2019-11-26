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

package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.schema.SchemaField;

/**
 * Accumulates stats separated by slot number for the fields with {@link org.apache.lucene.index.DocValues}
 */
public abstract class DocValuesAcc extends SlotAcc {
  SchemaField sf;

  public DocValuesAcc(FacetContext fcontext, SchemaField sf) throws IOException {
    super(fcontext);
    this.sf = sf;
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    int valuesDocID = docIdSetIterator().docID();
    if (valuesDocID < doc) {
      valuesDocID = docIdSetIterator().advance(doc);
    }
    if (valuesDocID > doc) {
      // missing
      return;
    }
    assert valuesDocID == doc;
    collectValues(doc, slot);
  }

  protected abstract void collectValues(int doc, int slot) throws IOException;

  protected abstract DocIdSetIterator docIdSetIterator();
}

/**
 * Accumulator for {@link NumericDocValues}
 */
abstract class NumericDVAcc extends DocValuesAcc {
  NumericDocValues values;

  public NumericDVAcc(FacetContext fcontext, SchemaField sf) throws IOException {
    super(fcontext, sf);
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    super.setNextReader(readerContext);
    values = DocValues.getNumeric(readerContext.reader(),  sf.getName());
  }

  @Override
  protected DocIdSetIterator docIdSetIterator() {
    return values;
  }
}

/**
 * Accumulator for {@link SortedNumericDocValues}
 */
abstract class SortedNumericDVAcc extends DocValuesAcc {
  SortedNumericDocValues values;

  public SortedNumericDVAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
    super(fcontext, sf);
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    super.setNextReader(readerContext);
    values = DocValues.getSortedNumeric(readerContext.reader(),  sf.getName());
  }

  @Override
  protected DocIdSetIterator docIdSetIterator() {
    return values;
  }
}

abstract class LongSortedNumericDVAcc extends SortedNumericDVAcc {
  long[] result;
  long initialValue;

  public LongSortedNumericDVAcc(FacetContext fcontext, SchemaField sf, int numSlots, long initialValue) throws IOException {
    super(fcontext, sf, numSlots);
    this.result = new long[numSlots];
    this.initialValue = initialValue;
    if (initialValue != 0) {
      Arrays.fill(result, initialValue);
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Long.compare(result[slotA], result[slotB]);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return result[slotNum];
  }

  @Override
  public void reset() throws IOException {
    Arrays.fill(result, initialValue);
  }

  @Override
  public void resize(Resizer resizer) {
    resizer.resize(result, initialValue);
  }

}

/**
 * Accumulator for {@link SortedDocValues}
 */
abstract class SortedDVAcc extends DocValuesAcc {
  SortedDocValues values;

  public SortedDVAcc(FacetContext fcontext, SchemaField sf) throws IOException {
    super(fcontext, sf);
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    super.setNextReader(readerContext);
    values = DocValues.getSorted(readerContext.reader(), sf.getName());
  }

  @Override
  protected DocIdSetIterator docIdSetIterator() {
    return values;
  }
}

/**
 * Accumulator for {@link SortedSetDocValues}
 */
abstract class SortedSetDVAcc extends DocValuesAcc {
  SortedSetDocValues values;

  public SortedSetDVAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
    super(fcontext, sf);
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    super.setNextReader(readerContext);
    values = DocValues.getSortedSet(readerContext.reader(), sf.getName());
  }

  @Override
  protected DocIdSetIterator docIdSetIterator() {
    return values;
  }
}

abstract class LongSortedSetDVAcc extends SortedSetDVAcc {
  long[] result;
  long initialValue;

  public LongSortedSetDVAcc(FacetContext fcontext, SchemaField sf, int numSlots, long initialValue) throws IOException {
    super(fcontext, sf, numSlots);
    result = new long[numSlots];
    this.initialValue = initialValue;
    if (initialValue != 0) {
      Arrays.fill(result, initialValue);
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Long.compare(result[slotA], result[slotB]);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return result[slotNum];
  }

  @Override
  public void reset() throws IOException {
    Arrays.fill(result, initialValue);
  }

  @Override
  public void resize(Resizer resizer) {
    resizer.resize(result, initialValue);
  }
}
