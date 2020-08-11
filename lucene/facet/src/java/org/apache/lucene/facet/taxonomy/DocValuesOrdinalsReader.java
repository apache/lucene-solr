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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/** Decodes ordinals previously indexed into a BinaryDocValues field */
public class DocValuesOrdinalsReader extends OrdinalsReader {
  private final String field;

  /** Default constructor. */
  public DocValuesOrdinalsReader() {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
  }

  /** Create this, with the specified indexed field name. */
  public DocValuesOrdinalsReader(String field) {
    this.field = field;
  }

  @Override
  public OrdinalsSegmentReader getReader(LeafReaderContext context) throws IOException {
    BinaryDocValues values0 = context.reader().getBinaryDocValues(field);
    if (values0 == null) {
      values0 = DocValues.emptyBinary();
    }

    final BinaryDocValues values = values0;

    return new OrdinalsSegmentReader() {

      private int lastDocID;
      
      @Override
      public void get(int docID, IntsRef ordinals) throws IOException {
        if (docID < lastDocID) {
          throw new AssertionError("docs out of order: lastDocID=" + lastDocID + " vs docID=" + docID);
        }
        lastDocID = docID;
        if (docID > values.docID()) {
          values.advance(docID);
        }
        final BytesRef bytes;
        if (values.docID() == docID) {
          bytes = values.binaryValue();
        } else {
          bytes = new BytesRef(BytesRef.EMPTY_BYTES);
        }
        decode(bytes, ordinals);
      }
    };
  }

  @Override
  public String getIndexFieldName() {
    return field;
  }

 /** 
   * Subclass and override if you change the encoding. 
   * The method is marked 'public' to allow decoding of binary payload containing ordinals
   * without instantiating an {@link org.apache.lucene.facet.taxonomy.OrdinalsReader.OrdinalsSegmentReader}.
   *  
   * This takes care of use cases where an application instantiates {@link org.apache.lucene.index.BinaryDocValues} 
   * reader for a facet field outside this class, reads the binary payload for a document and decodes the ordinals
   * in the payload.
   *
   * @param buf binary payload containing encoded ordinals
   * @param ordinals buffer for decoded ordinals
   */
  public void decode(BytesRef buf, IntsRef ordinals) {

    // grow the buffer up front, even if by a large number of values (buf.length)
    // that saves the need to check inside the loop for every decoded value if
    // the buffer needs to grow.
    if (ordinals.ints.length < buf.length) {
      ordinals.ints = ArrayUtil.grow(ordinals.ints, buf.length);
    }

    ordinals.offset = 0;
    ordinals.length = 0;

    // it is better if the decoding is inlined like so, and not e.g.
    // in a utility method
    int upto = buf.offset + buf.length;
    int value = 0;
    int offset = buf.offset;
    int prev = 0;
    while (offset < upto) {
      byte b = buf.bytes[offset++];
      if (b >= 0) {
        ordinals.ints[ordinals.length] = ((value << 7) | b) + prev;
        value = 0;
        prev = ordinals.ints[ordinals.length];
        ordinals.length++;
      } else {
        value = (value << 7) | (b & 0x7F);
      }
    }
  }
}
