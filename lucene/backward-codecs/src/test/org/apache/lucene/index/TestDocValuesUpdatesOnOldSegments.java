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
package org.apache.lucene.index;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40RWCodec;
import org.apache.lucene.codecs.lucene41.Lucene41RWCodec;
import org.apache.lucene.codecs.lucene42.Lucene42RWCodec;
import org.apache.lucene.codecs.lucene45.Lucene45RWCodec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/** 
 * Tests performing docvalues updates against versions of lucene
 * that did not support it.
 */
public class TestDocValuesUpdatesOnOldSegments extends LuceneTestCase {

  static long getValue(BinaryDocValues bdv, int idx) {
    BytesRef term = bdv.get(idx);
    idx = term.offset;
    byte b = term.bytes[idx++];
    long value = b & 0x7FL;
    for (int shift = 7; (b & 0x80L) != 0; shift += 7) {
      b = term.bytes[idx++];
      value |= (b & 0x7FL) << shift;
    }
    return value;
  }

  // encodes a long into a BytesRef as VLong so that we get varying number of bytes when we update
  static BytesRef toBytes(long value) {
    BytesRef bytes = new BytesRef(10); // negative longs may take 10 bytes
    while ((value & ~0x7FL) != 0L) {
      bytes.bytes[bytes.length++] = (byte) ((value & 0x7FL) | 0x80L);
      value >>>= 7;
    }
    bytes.bytes[bytes.length++] = (byte) value;
    return bytes;
  }

  public void testBinaryUpdates() throws Exception {
    Codec[] oldCodecs = new Codec[] { new Lucene40RWCodec(), new Lucene41RWCodec(), new Lucene42RWCodec(), new Lucene45RWCodec() };
    
    for (Codec codec : oldCodecs) {
      Directory dir = newDirectory();
      
      // create a segment with an old Codec
      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
      conf.setCodec(codec);
      IndexWriter writer = new IndexWriter(dir, conf);
      Document doc = new Document();
      doc.add(new StringField("id", "doc", Store.NO));
      doc.add(new BinaryDocValuesField("f", toBytes(5L)));
      writer.addDocument(doc);
      writer.close();
      
      conf = newIndexWriterConfig(new MockAnalyzer(random()));
      writer = new IndexWriter(dir, conf);
      writer.updateBinaryDocValue(new Term("id", "doc"), "f", toBytes(4L));
      try {
        writer.close();
        fail("should not have succeeded to update a segment written with an old Codec");
      } catch (UnsupportedOperationException e) {
        writer.rollback();
      }
      
      dir.close();
    }
  }

  public void testNumericUpdates() throws Exception {
    Codec[] oldCodecs = new Codec[] { new Lucene40RWCodec(), new Lucene41RWCodec(), new Lucene42RWCodec(), new Lucene45RWCodec() };
    
    for (Codec codec : oldCodecs) {
      Directory dir = newDirectory();
      
      // create a segment with an old Codec
      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
      conf.setCodec(codec);
      IndexWriter writer = new IndexWriter(dir, conf);
      Document doc = new Document();
      doc.add(new StringField("id", "doc", Store.NO));
      doc.add(new NumericDocValuesField("f", 5));
      writer.addDocument(doc);
      writer.close();
      
      conf = newIndexWriterConfig(new MockAnalyzer(random()));
      writer = new IndexWriter(dir, conf);
      writer.updateNumericDocValue(new Term("id", "doc"), "f", 4L);
      try {
        writer.close();
        fail("should not have succeeded to update a segment written with an old Codec");
      } catch (UnsupportedOperationException e) {
        writer.rollback();
      }
      
      dir.close();
    }
  }

}
