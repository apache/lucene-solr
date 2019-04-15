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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.MapWriter;
import org.apache.solr.schema.FieldType;

class BoolFieldWriter extends FieldWriter {
  private String field;
  private FieldType fieldType;
  private CharsRefBuilder cref = new CharsRefBuilder();

  public BoolFieldWriter(String field, FieldType fieldType) {
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
      SortedDocValues vals = DocValues.getSorted(reader, this.field);
      if (vals.advance(sortDoc.docId) != sortDoc.docId) {
        return false;
      }
      int ord = vals.ordValue();
      ref = vals.lookupOrd(ord);
    }

    fieldType.indexedToReadable(ref, cref);
    ew.put(this.field, "true".equals(cref.toString()));
    return true;
  }
}
