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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.schema.FieldType;

class StringFieldWriter extends FieldWriter {
  private String field;
  private FieldType fieldType;
  private Map<Integer, SortedDocValues> lastDocValues = new HashMap<>();
  private CharsRefBuilder cref = new CharsRefBuilder();
  final ByteArrayUtf8CharSequence utf8 = new ByteArrayUtf8CharSequence(new byte[0], 0, 0) {
    @Override
    public String toString() {
      String str = super.utf16;
      if (str != null) return str;
      fieldType.indexedToReadable(new BytesRef(super.buf, super.offset, super.length), cref);
      str = cref.toString();
      super.utf16 = str;
      return str;
    }
  };

  public StringFieldWriter(String field, FieldType fieldType) {
    this.field = field;
    this.fieldType = fieldType;
  }

  public boolean write(SortDoc sortDoc, LeafReader reader, MapWriter.EntryWriter ew, int fieldIndex) throws IOException {
    BytesRef ref;
    SortValue sortValue = sortDoc.getSortValue(this.field);
    if (sortValue != null) {
      if (sortValue.isPresent()) {
        ref = (BytesRef) sortValue.getCurrentValue();
      } else { //empty-value
        return false;
      }
    } else {
      // field is not part of 'sort' param, but part of 'fl' param
      SortedDocValues vals = lastDocValues.get(sortDoc.ord);
      if (vals == null || vals.docID() >= sortDoc.docId) {
        vals = DocValues.getSorted(reader, this.field);
        lastDocValues.put(sortDoc.ord, vals);
      }
      if (vals.advance(sortDoc.docId) != sortDoc.docId) {
        return false;
      }
      int ord = vals.ordValue();
      ref = vals.lookupOrd(ord);
    }

    if (ew instanceof JavaBinCodec.BinEntryWriter) {
      ew.put(this.field, utf8.reset(ref.bytes, ref.offset, ref.length, null));
    } else {
      String v = null;
      if (sortValue != null) {
        v = ((StringValue) sortValue).getLastString();
        if (v == null) {
          fieldType.indexedToReadable(ref, cref);
          v = cref.toString();
          ((StringValue) sortValue).setLastString(v);
        }
      } else {
        fieldType.indexedToReadable(ref, cref);
        v = cref.toString();
      }

      ew.put(this.field, v);

    }
    return true;
  }
}