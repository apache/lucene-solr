package org.apache.lucene.codecs.compressing;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.lucene41.Lucene41Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

public class TestCompressingStoredFieldsFormat extends LuceneTestCase {

  public void testWriteReadMerge() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    iwConf.setCodec(CompressingCodec.randomInstance(random()));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    final int docCount = atLeast(200);
    final byte[][][] data = new byte [docCount][][];
    for (int i = 0; i < docCount; ++i) {
      final int fieldCount = rarely()
          ? RandomInts.randomIntBetween(random(), 1, 500)
          : RandomInts.randomIntBetween(random(), 1, 5);
      data[i] = new byte[fieldCount][];
      for (int j = 0; j < fieldCount; ++j) {
        final int length = rarely()
            ? random().nextInt(1000)
            : random().nextInt(10);
        final byte[] arr = new byte[length];
        final int max = rarely() ? 256 : 2;
        for (int k = 0; k < length; ++k) {
          arr[k] = (byte) random().nextInt(max);
        }
        data[i][j] = arr;
      }
    }

    final FieldType type = new FieldType(StringField.TYPE_STORED);
    type.setIndexed(false);
    type.freeze();
    IntField id = new IntField("id", 0, Store.YES);
    for (int i = 0; i < data.length; ++i) {
      Document doc = new Document();
      doc.add(id);
      id.setIntValue(i);
      for (int j = 0; j < data[i].length; ++j) {
        Field f = new Field("bytes" + j, data[i][j], type);
        doc.add(f);
      }
      iw.w.addDocument(doc);
      if (random().nextBoolean() && (i % (data.length / 10) == 0)) {
        iw.w.close();
        // switch codecs
        if (iwConf.getCodec() instanceof Lucene41Codec) {
          iwConf.setCodec(CompressingCodec.randomInstance(random()));
        } else {
          iwConf.setCodec(new Lucene41Codec());
        }
        iw = new RandomIndexWriter(random(), dir, iwConf);
      }
    }

    for (int i = 0; i < 10; ++i) {
      final int min = random().nextInt(data.length);
      final int max = min + random().nextInt(20);
      iw.deleteDocuments(NumericRangeQuery.newIntRange("id", min, max, true, false));
    }

    iw.forceMerge(2); // force merges with deletions

    iw.commit();

    final DirectoryReader ir = DirectoryReader.open(dir);
    assertTrue(ir.numDocs() > 0);
    int numDocs = 0;
    for (int i = 0; i < ir.maxDoc(); ++i) {
      final Document doc = ir.document(i);
      if (doc == null) {
        continue;
      }
      ++ numDocs;
      final int docId = doc.getField("id").numericValue().intValue();
      assertEquals(data[docId].length + 1, doc.getFields().size());
      for (int j = 0; j < data[docId].length; ++j) {
        final byte[] arr = data[docId][j];
        final BytesRef arr2Ref = doc.getBinaryValue("bytes" + j);
        final byte[] arr2 = Arrays.copyOfRange(arr2Ref.bytes, arr2Ref.offset, arr2Ref.offset + arr2Ref.length);
        assertArrayEquals(arr, arr2);
      }
    }
    assertTrue(ir.numDocs() <= numDocs);
    ir.close();

    iw.deleteAll();
    iw.commit();
    iw.forceMerge(1);
    iw.close();
    dir.close();
  }

  public void testReadSkip() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    iwConf.setCodec(CompressingCodec.randomInstance(random()));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);

    FieldType ft = new FieldType();
    ft.setStored(true);
    ft.freeze();
    
    final String string = _TestUtil.randomSimpleString(random(), 50);
    final byte[] bytes = string.getBytes("UTF-8");
    final long l = random().nextBoolean() ? random().nextInt(42) : random().nextLong();
    final int i = random().nextBoolean() ? random().nextInt(42) : random().nextInt();
    final float f = random().nextFloat();
    final double d = random().nextDouble();

    List<Field> fields = Arrays.asList(
        new Field("bytes", bytes, ft),
        new Field("string", string, ft),
        new LongField("long", l, Store.YES),
        new IntField("int", i, Store.YES),
        new FloatField("float", f, Store.YES),
        new DoubleField("double", d, Store.YES)
    );

    for (int k = 0; k < 100; ++k) {
      Document doc = new Document();
      for (Field fld : fields) {
        doc.add(fld);
      }
      iw.w.addDocument(doc);
    }
    iw.close();

    final DirectoryReader reader = DirectoryReader.open(dir);
    final int docID = random().nextInt(100);
    for (Field fld : fields) {
      String fldName = fld.name();
      final Document sDoc = reader.document(docID, Collections.singleton(fldName));
      final IndexableField sField = sDoc.getField(fldName);
      if (Field.class.equals(fld.getClass())) {
        assertEquals(fld.binaryValue(), sField.binaryValue());
        assertEquals(fld.stringValue(), sField.stringValue());
      } else {
        assertEquals(fld.numericValue(), sField.numericValue());
      }
    }
    reader.close();
    dir.close();
  }

}
