package org.apache.lucene.index;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.ByteDocValuesField;
import org.apache.lucene.document.DerefBytesDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.IntDocValuesField;
import org.apache.lucene.document.LongDocValuesField;
import org.apache.lucene.document.PackedLongDocValuesField;
import org.apache.lucene.document.ShortDocValuesField;
import org.apache.lucene.document.SortedBytesDocValuesField;
import org.apache.lucene.document.StraightBytesDocValuesField;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;

/**
 * Tests compatibility of {@link DocValues.Type} during indexing
 */
public class TestDocValuesTypeCompatibility extends LuceneTestCase {
  
  public void testAddCompatibleIntTypes() throws IOException {
    int numIter = atLeast(10);
    for (int i = 0; i < numIter; i++) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT,
          new MockAnalyzer(random()));
      int numDocs = atLeast(100);
      
      iwc.setMaxBufferedDocs(2 * numDocs); // make sure we hit the same DWPT
                                           // here
      iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      iwc.setRAMPerThreadHardLimitMB(2000);
      IndexWriter writer = new IndexWriter(dir, iwc);
      Type[] types = new Type[] {Type.VAR_INTS, Type.FIXED_INTS_16,
          Type.FIXED_INTS_64, Type.FIXED_INTS_16, Type.FIXED_INTS_8};
      Type maxType = types[random().nextInt(types.length)];
      for (int j = 0; j < numDocs; j++) {
        addDoc(writer, getRandomIntsField(maxType, j == 0));
      }
      writer.close();
      dir.close();
    }
    
  }
  
  @SuppressWarnings("fallthrough")
  public Field getRandomIntsField(Type maxType, boolean force) {
    switch (maxType) {
    
      case VAR_INTS:
        if (random().nextInt(5) == 0 || force) {
          return new PackedLongDocValuesField("f", 1);
        }
      case FIXED_INTS_64:
        if (random().nextInt(4) == 0 || force) {
          return new LongDocValuesField("f", 1);
        }
      case FIXED_INTS_32:
        if (random().nextInt(3) == 0 || force) {
          return new IntDocValuesField("f", 1);
        }
      case FIXED_INTS_16:
        if (random().nextInt(2) == 0 || force) {
          return new ShortDocValuesField("f", (short) 1);
        }
      case FIXED_INTS_8:
        return new ByteDocValuesField("f", (byte) 1);
        
      default:
        throw new IllegalArgumentException();
        
    }
  }
  
  public void testAddCompatibleDoubleTypes() throws IOException {
    int numIter = atLeast(10);
    for (int i = 0; i < numIter; i++) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT,
          new MockAnalyzer(random()));
      int numDocs = atLeast(100);
      
      iwc.setMaxBufferedDocs(2 * numDocs); // make sure we hit the same DWPT
                                           // here
      iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      iwc.setRAMPerThreadHardLimitMB(2000);
      IndexWriter writer = new IndexWriter(dir, iwc);
      
      Type[] types = new Type[] {Type.FLOAT_64, Type.FLOAT_32};
      Type maxType = types[random().nextInt(types.length)];
      for (int j = 0; j < numDocs; j++) {
        addDoc(writer, getRandomFloatField(maxType, j == 0));
      }
      writer.close();
      dir.close();
    }
    
  }
  @SuppressWarnings("fallthrough")
  public Field getRandomFloatField(Type maxType, boolean force) {
    switch (maxType) {
    
      case FLOAT_64:
        if (random().nextInt(5) == 0 || force) {
          return new PackedLongDocValuesField("f", 1);
        }
      case FIXED_INTS_32:
        if (random().nextInt(4) == 0 || force) {
          return new LongDocValuesField("f", 1);
        }
      case FLOAT_32:
        if (random().nextInt(3) == 0 || force) {
          return new IntDocValuesField("f", 1);
        }
      case FIXED_INTS_16:
        if (random().nextInt(2) == 0 || force) {
          return new ShortDocValuesField("f", (short) 1);
        }
      case FIXED_INTS_8:
        return new ByteDocValuesField("f", (byte) 1);
        
      default:
        throw new IllegalArgumentException();
        
    }
  }
  
  public void testAddCompatibleDoubleTypes2() throws IOException {
    int numIter = atLeast(10);
    for (int i = 0; i < numIter; i++) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT,
          new MockAnalyzer(random()));
      int numDocs = atLeast(100);
      
      iwc.setMaxBufferedDocs(2 * numDocs); // make sure we hit the same DWPT
                                           // here
      iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      iwc.setRAMPerThreadHardLimitMB(2000);
      IndexWriter writer = new IndexWriter(dir, iwc);
      Field[] fields = new Field[] {
          new DoubleDocValuesField("f", 1.0), new IntDocValuesField("f", 1),
          new ShortDocValuesField("f", (short) 1),
          new ByteDocValuesField("f", (byte) 1)};
      int base = random().nextInt(fields.length - 1);
      
      addDoc(writer, fields[base]);
      
      for (int j = 0; j < numDocs; j++) {
        int f = base + random().nextInt(fields.length - base);
        addDoc(writer, fields[f]);
      }
      writer.close();
      dir.close();
    }
    
  }

  // nocommit remove this test?  simple dv doesn't let you
  // change b/w sorted & binary?
  @Ignore
  public void testAddCompatibleByteTypes() throws IOException {
    int numIter = atLeast(10);
    for (int i = 0; i < numIter; i++) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT,
          new MockAnalyzer(random()));
      int numDocs = atLeast(100);
      
      iwc.setMaxBufferedDocs(2 * numDocs); // make sure we hit the same DWPT
                                           // here
      iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      iwc.setRAMPerThreadHardLimitMB(2000);
      IndexWriter writer = new IndexWriter(dir, iwc);
      boolean mustBeFixed = random().nextBoolean();
      int maxSize = 2 + random().nextInt(15);
      Field bytesField = getRandomBytesField(mustBeFixed, maxSize,
          true);
      addDoc(writer, bytesField);
      for (int j = 0; j < numDocs; j++) {
        bytesField = getRandomBytesField(mustBeFixed, maxSize, false);
        addDoc(writer, bytesField);
        
      }
      writer.close();
      dir.close();
    }
  }
  
  public Field getRandomBytesField(boolean mustBeFixed, int maxSize,
      boolean mustBeVariableIfNotFixed) {
    int size = mustBeFixed ? maxSize : random().nextInt(maxSize) + 1;
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < size; i++) {
      s.append("a");
    }
    BytesRef bytesRef = new BytesRef(s);
    boolean fixed = mustBeFixed ? true : mustBeVariableIfNotFixed ? false
        : random().nextBoolean();
    switch (random().nextInt(3)) {
      case 0:
        return new SortedBytesDocValuesField("f", bytesRef, fixed);
      case 1:
        return new DerefBytesDocValuesField("f", bytesRef, fixed);
      default:
        return new StraightBytesDocValuesField("f", bytesRef, fixed);
    }
  }
  
  public void testIncompatibleTypesBytes() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    int numDocs = atLeast(100);

    iwc.setMaxBufferedDocs(numDocs); // make sure we hit the same DWPT
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setRAMPerThreadHardLimitMB(2000);
    IndexWriter writer = new IndexWriter(dir, iwc);
    
    int numDocsIndexed = 0;
    for (int j = 1; j < numDocs; j++) {
      try {
        addDoc(writer, getRandomIndexableDVField());
        numDocsIndexed++;
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().startsWith("Incompatible DocValues type:"));
      }
    }
    
    writer.commit();
    DirectoryReader open = DirectoryReader.open(dir);
    assertEquals(numDocsIndexed, open.numDocs());
    open.close();
    writer.close();
    dir.close();
  }
  
  private void addDoc(IndexWriter writer, Field... fields)
      throws IOException {
    Document doc = new Document();
    for (Field indexableField : fields) {
      doc.add(indexableField);
    }
    writer.addDocument(doc);
  }
  
  public Field getRandomIndexableDVField() {
    int size = random().nextInt(100) + 1;
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < size; i++) {
      s.append("a");
    }
    BytesRef bytesRef = new BytesRef(s);
    
    Type[] values = Type.values();
    Type t = values[random().nextInt(values.length)];
    switch (t) {
      case BYTES_FIXED_DEREF:
        return new DerefBytesDocValuesField("f", bytesRef, true);
      case BYTES_FIXED_SORTED:
        return new SortedBytesDocValuesField("f", bytesRef, true);
      case BYTES_FIXED_STRAIGHT:
        return new StraightBytesDocValuesField("f", bytesRef, true);
      case BYTES_VAR_DEREF:
        return new DerefBytesDocValuesField("f", bytesRef, false);
      case BYTES_VAR_SORTED:
        return new SortedBytesDocValuesField("f", bytesRef, false);
      case BYTES_VAR_STRAIGHT:
        return new StraightBytesDocValuesField("f", bytesRef, false);
      case FIXED_INTS_16:
        return new ShortDocValuesField("f", (short) 1);
      case FIXED_INTS_32:
        return new IntDocValuesField("f", 1);
      case FIXED_INTS_64:
        return new LongDocValuesField("f", 1);
      case FIXED_INTS_8:
        return new ByteDocValuesField("f", (byte) 1);
      case FLOAT_32:
        return new FloatDocValuesField("f", 1.0f);
      case FLOAT_64:
        return new DoubleDocValuesField("f", 1.0f);
      case VAR_INTS:
        return new PackedLongDocValuesField("f", 1);
      default:
        throw new IllegalArgumentException();
        
    }
    
  }
  
}
