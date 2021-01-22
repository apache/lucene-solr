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

package org.apache.solr.handler.export;

import java.io.IOException;
import java.util.Date;
import java.util.function.LongFunction;

import com.carrotsearch.hppc.IntObjectHashMap;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

class MultiFieldWriter extends FieldWriter {
  private String field;
  private FieldType fieldType;
  private SchemaField schemaField;
  private boolean numeric;
  private CharsRefBuilder cref = new CharsRefBuilder();
  private final LongFunction<Object> bitsToValue;
  private IntObjectHashMap<Object> docValuesCache = new IntObjectHashMap<>();


  public MultiFieldWriter(String field, FieldType fieldType, SchemaField schemaField, boolean numeric) {
    this.field = field;
    this.fieldType = fieldType;
    this.schemaField = schemaField;
    this.numeric = numeric;
    if (this.fieldType.isPointField()) {
      bitsToValue = bitsToValue(fieldType);
    } else {
      bitsToValue = null;
    }
  }

  public boolean write(SortDoc sortDoc, LeafReaderContext readerContext, MapWriter.EntryWriter out, int fieldIndex) throws IOException {
    if (this.fieldType.isPointField()) {
      int readerOrd = readerContext.ord;
      SortedNumericDocValues vals = null;
      if(docValuesCache.containsKey(readerOrd)) {
        SortedNumericDocValues sortedNumericDocValues = (SortedNumericDocValues) docValuesCache.get(readerOrd);
        if(sortedNumericDocValues.docID() < sortDoc.docId) {
          //We have not advanced beyond the current docId so we can use this docValues.
          vals = sortedNumericDocValues;
        }
      }

      if(vals == null) {
        vals = DocValues.getSortedNumeric(readerContext.reader(), this.field);
        docValuesCache.put(readerOrd, vals);
      }

      if (!vals.advanceExact(sortDoc.docId)) return false;

      final SortedNumericDocValues docVals = vals;

      out.put(this.field,
          (IteratorWriter) w -> {
            for (int i = 0, count = docVals.docValueCount(); i < count; i++) {
              w.add(bitsToValue.apply(docVals.nextValue()));
            }
          });
      return true;
    } else {
      int readerOrd = readerContext.ord;
      SortedSetDocValues vals = null;
      if(docValuesCache.containsKey(readerOrd)) {
        SortedSetDocValues sortedSetDocValues = (SortedSetDocValues) docValuesCache.get(readerOrd);
        if(sortedSetDocValues.docID() < sortDoc.docId) {
          //We have not advanced beyond the current docId so we can use this docValues.
          vals = sortedSetDocValues;
        }
      }

      if(vals == null) {
        vals = DocValues.getSortedSet(readerContext.reader(), this.field);
        docValuesCache.put(readerOrd, vals);
      }

      if (vals.advance(sortDoc.docId) != sortDoc.docId) return false;

      final SortedSetDocValues docVals = vals;

      out.put(this.field,
          (IteratorWriter) w -> {
            long o;
            while((o = docVals.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              BytesRef ref = docVals.lookupOrd(o);
              fieldType.indexedToReadable(ref, cref);
              IndexableField f = fieldType.createField(schemaField, cref.toString());
              if (f == null) w.add(cref.toString());
              else w.add(fieldType.toObject(f));
            }
          });
      return true;
    }

  }


  static LongFunction<Object> bitsToValue(FieldType fieldType) {
    switch (fieldType.getNumberType()) {
      case LONG:
        return (bits)-> bits;
      case DATE:
        return (bits)-> new Date(bits);
      case INTEGER:
        return (bits)-> (int)bits;
      case FLOAT:
        return (bits)-> NumericUtils.sortableIntToFloat((int)bits);
      case DOUBLE:
        return (bits)-> NumericUtils.sortableLongToDouble(bits);
      default:
        throw new AssertionError("Unsupported NumberType: " + fieldType.getNumberType());
    }
  }
}
