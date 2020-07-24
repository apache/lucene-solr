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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;

public class StringRefValueSource extends FieldCacheSource {

  private final SchemaField schemaField;
  private final FieldType fieldType;
  private final SortedSetSelector.Type selectorType;

  public StringRefValueSource(SchemaField schemaField) {
    this(schemaField, SortedSetSelector.Type.MIN);
  }
  public StringRefValueSource(SchemaField schemaField, SortedSetSelector.Type selectorType) {
    super(schemaField.getName());
    this.schemaField = schemaField;
    this.fieldType = schemaField.getType();
    this.selectorType = selectorType;
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext) throws IOException {
    DocValuesRefIterator backing = fieldType.getDocValuesRefIterator(readerContext.reader(), schemaField);
    switch (selectorType) {
      case MIN:
        // will work with regular dvRefIter, since advance only reads the first BytesRef anyway
        break;
      case MAX:
        // wrap with a shim that returns the last value for a given doc
        backing = new MaxDocValuesRefIterator(backing);
        break;
      default:
        throw new IllegalStateException("unsupported selectorType: "+selectorType);
    }
    return new StringRefFunctionValues(field, fieldType, backing);
  }

  private static class MaxDocValuesRefIterator extends DocValuesRefIterator {
    private final DocValuesRefIterator backing;
    private MaxDocValuesRefIterator(DocValuesRefIterator backing) {
      this.backing = backing;
    }
    @Override
    public BytesRef nextRef() throws IOException {
      BytesRef last = null;
      BytesRef next;
      while ((next = backing.nextRef()) != null) {
        last = next;
      }
      return last;
    }
    @Override
    public boolean advanceExact(int target) throws IOException {
      return backing.advanceExact(target);
    }
    @Override
    public int docID() {
      return backing.docID();
    }
    @Override
    public int nextDoc() throws IOException {
      return backing.nextDoc();
    }
    @Override
    public int advance(int target) throws IOException {
      return backing.advance(target);
    }
    @Override
    public long cost() {
      return backing.cost();
    }
  }
  private static class StringRefFunctionValues extends FunctionValues {

    private final String fieldName;
    private final FieldType fieldType;
    private final DocValuesRefIterator refIter;
    private final CharsRefBuilder cref = new CharsRefBuilder();
    private int lastDocID = -1;
    private BytesRef cachedRef;

    private StringRefFunctionValues(String fieldName, FieldType fieldType, DocValuesRefIterator refIter) {
      this.fieldName = fieldName;
      this.fieldType = fieldType;
      this.refIter = refIter;
    }
    private BytesRef getValueForDoc(int doc) throws IOException {
      if (doc < lastDocID) {
        throw new IllegalArgumentException("docs were sent out-of-order: lastDocID=" + lastDocID + " vs docID=" + doc);
      }
      lastDocID = doc;
      int curDocID = refIter.docID();
      if (curDocID < 0 || doc > curDocID) {
        cachedRef = null;
        curDocID = refIter.advance(doc);
      }
      if (doc == curDocID) {
        return cachedRef != null ? cachedRef : (cachedRef = refIter.nextRef());
      } else {
        return null;
      }
    }

    @Override
    public boolean exists(int doc) throws IOException {
      return getValueForDoc(doc) != null;
    }

    @Override
    public boolean bytesVal(int doc, BytesRefBuilder target) throws IOException {
      BytesRef value = getValueForDoc(doc);
      if (value == null || value.length == 0) {
        return false;
      } else {
        target.copyBytes(value);
        return true;
      }
    }

    public String strVal(int doc) throws IOException {
      BytesRef value = getValueForDoc(doc);
      if (value == null || value.length == 0) {
        return null;
      } else {
        cref.clear();
        fieldType.indexedToReadable(value, cref);
        return cref.toString();
      }
    }

    @Override
    public Object objectVal(int doc) throws IOException {
      return strVal(doc);
    }

    @Override
    public String toString(int doc) throws IOException {
      return fieldName + '=' + strVal(doc);
    }

    @Override
    public ValueFiller getValueFiller() {
      return new ValueFiller() {
        private final MutableValueStr mval = new MutableValueStr();

        @Override
        public MutableValue getValue() {
          return mval;
        }

        @Override
        public void fillValue(int doc) throws IOException {
          BytesRef value = getValueForDoc(doc);
          mval.exists = value != null;
          mval.value.clear();
          if (value != null) {
            mval.value.copyBytes(value);
          }
        }
      };
    }
  }
}
