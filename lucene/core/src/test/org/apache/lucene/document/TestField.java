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
package org.apache.lucene.document;


import java.io.StringReader;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

// sanity check some basics of fields
public class TestField extends LuceneTestCase {
  
  public void testDoubleField() throws Exception {
    Field fields[] = new Field[] {
        new DoubleField("foo", 5d, Field.Store.NO),
        new DoubleField("foo", 5d, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetBoost(field);
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      field.setDoubleValue(6d); // ok
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
    
      assertEquals(6d, field.numericValue().doubleValue(), 0.0d);
    }
  }
  
  public void testDoubleDocValuesField() throws Exception {
    DoubleDocValuesField field = new DoubleDocValuesField("foo", 5d);

    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    field.setDoubleValue(6d); // ok
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(6d, Double.longBitsToDouble(field.numericValue().longValue()), 0.0d);
  }
  
  public void testFloatDocValuesField() throws Exception {
    FloatDocValuesField field = new FloatDocValuesField("foo", 5f);

    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    field.setFloatValue(6f); // ok
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(6f, Float.intBitsToFloat(field.numericValue().intValue()), 0.0f);
  }
  
  public void testFloatField() throws Exception {
    Field fields[] = new Field[] {
        new FloatField("foo", 5f, Field.Store.NO),
        new FloatField("foo", 5f, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetBoost(field);
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      field.setFloatValue(6f); // ok
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
      
      assertEquals(6f, field.numericValue().floatValue(), 0.0f);
    }
  }
  
  public void testIntField() throws Exception {
    Field fields[] = new Field[] {
        new IntField("foo", 5, Field.Store.NO),
        new IntField("foo", 5, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetBoost(field);
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      field.setIntValue(6); // ok
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
      
      assertEquals(6, field.numericValue().intValue());
    }
  }
  
  public void testNumericDocValuesField() throws Exception {
    NumericDocValuesField field = new NumericDocValuesField("foo", 5L);

    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    field.setLongValue(6); // ok
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(6L, field.numericValue().longValue());
  }
  
  public void testLongField() throws Exception {
    Field fields[] = new Field[] {
        new LongField("foo", 5L, Field.Store.NO),
        new LongField("foo", 5L, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetBoost(field);
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      field.setLongValue(6); // ok
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
      
      assertEquals(6L, field.numericValue().longValue());
    }
  }
  
  public void testSortedBytesDocValuesField() throws Exception {
    SortedDocValuesField field = new SortedDocValuesField("foo", new BytesRef("bar"));

    trySetBoost(field);
    trySetByteValue(field);
    field.setBytesValue("fubar".getBytes(StandardCharsets.UTF_8));
    field.setBytesValue(new BytesRef("baz"));
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(new BytesRef("baz"), field.binaryValue());
  }
  
  public void testBinaryDocValuesField() throws Exception {
    BinaryDocValuesField field = new BinaryDocValuesField("foo", new BytesRef("bar"));

    trySetBoost(field);
    trySetByteValue(field);
    field.setBytesValue("fubar".getBytes(StandardCharsets.UTF_8));
    field.setBytesValue(new BytesRef("baz"));
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(new BytesRef("baz"), field.binaryValue());
  }
  
  public void testStringField() throws Exception {
    Field fields[] = new Field[] {
        new StringField("foo", "bar", Field.Store.NO),
        new StringField("foo", "bar", Field.Store.YES)
    };

    for (Field field : fields) {
      trySetBoost(field);
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      field.setStringValue("baz");
      trySetTokenStreamValue(field);
      
      assertEquals("baz", field.stringValue());
    }
  }
  
  public void testTextFieldString() throws Exception {
    Field fields[] = new Field[] {
        new TextField("foo", "bar", Field.Store.NO),
        new TextField("foo", "bar", Field.Store.YES)
    };

    for (Field field : fields) {
      field.setBoost(5f);
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      field.setStringValue("baz");
      field.setTokenStream(new CannedTokenStream(new Token("foo", 0, 3)));
      
      assertEquals("baz", field.stringValue());
      assertEquals(5f, field.boost(), 0f);
    }
  }
  
  public void testTextFieldReader() throws Exception {
    Field field = new TextField("foo", new StringReader("bar"));

    field.setBoost(5f);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    field.setReaderValue(new StringReader("foobar"));
    trySetShortValue(field);
    trySetStringValue(field);
    field.setTokenStream(new CannedTokenStream(new Token("foo", 0, 3)));
      
    assertNotNull(field.readerValue());
    assertEquals(5f, field.boost(), 0f);
  }
  
  /* TODO: this is pretty expert and crazy
   * see if we can fix it up later
  public void testTextFieldTokenStream() throws Exception {
  }
  */
  
  public void testStoredFieldBytes() throws Exception {
    Field fields[] = new Field[] {
        new StoredField("foo", "bar".getBytes(StandardCharsets.UTF_8)),
        new StoredField("foo", "bar".getBytes(StandardCharsets.UTF_8), 0, 3),
        new StoredField("foo", new BytesRef("bar")),
    };
    
    for (Field field : fields) {
      trySetBoost(field);
      trySetByteValue(field);
      field.setBytesValue("baz".getBytes(StandardCharsets.UTF_8));
      field.setBytesValue(new BytesRef("baz"));
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
      
      assertEquals(new BytesRef("baz"), field.binaryValue());
    }
  }
  
  public void testStoredFieldString() throws Exception {
    Field field = new StoredField("foo", "bar");
    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    field.setStringValue("baz");
    trySetTokenStreamValue(field);
    
    assertEquals("baz", field.stringValue());
  }
  
  public void testStoredFieldInt() throws Exception {
    Field field = new StoredField("foo", 1);
    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    field.setIntValue(5);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(5, field.numericValue().intValue());
  }
  
  public void testStoredFieldDouble() throws Exception {
    Field field = new StoredField("foo", 1D);
    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    field.setDoubleValue(5D);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(5D, field.numericValue().doubleValue(), 0.0D);
  }
  
  public void testStoredFieldFloat() throws Exception {
    Field field = new StoredField("foo", 1F);
    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    field.setFloatValue(5f);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(5f, field.numericValue().floatValue(), 0.0f);
  }
  
  public void testStoredFieldLong() throws Exception {
    Field field = new StoredField("foo", 1L);
    trySetBoost(field);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    field.setLongValue(5);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);
    
    assertEquals(5L, field.numericValue().longValue());
  }

  public void testIndexedBinaryField() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    BytesRef br = new BytesRef(new byte[5]);
    Field field = new StringField("binary", br, Field.Store.YES);
    assertEquals(br, field.binaryValue());
    doc.add(field);
    w.addDocument(doc);
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new TermQuery(new Term("binary", br)), 1);
    assertEquals(1, hits.totalHits);
    Document storedDoc = s.doc(hits.scoreDocs[0].doc);
    assertEquals(br, storedDoc.getField("binary").binaryValue());

    r.close();
    w.close();
    dir.close();
  }
  
  private void trySetByteValue(Field f) {
    try {
      f.setByteValue((byte) 10);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  private void trySetBytesValue(Field f) {
    try {
      f.setBytesValue(new byte[] { 5, 5 });
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetBytesRefValue(Field f) {
    try {
      f.setBytesValue(new BytesRef("bogus"));
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetDoubleValue(Field f) {
    try {
      f.setDoubleValue(Double.MAX_VALUE);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetIntValue(Field f) {
    try {
      f.setIntValue(Integer.MAX_VALUE);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetLongValue(Field f) {
    try {
      f.setLongValue(Long.MAX_VALUE);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetFloatValue(Field f) {
    try {
      f.setFloatValue(Float.MAX_VALUE);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetReaderValue(Field f) {
    try {
      f.setReaderValue(new StringReader("BOO!"));
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetShortValue(Field f) {
    try {
      f.setShortValue(Short.MAX_VALUE);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetStringValue(Field f) {
    try {
      f.setStringValue("BOO!");
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetTokenStreamValue(Field f) {
    try {
      f.setTokenStream(new CannedTokenStream(new Token("foo", 0, 3)));
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  private void trySetBoost(Field f) {
    try {
      f.setBoost(5.0f);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
}
