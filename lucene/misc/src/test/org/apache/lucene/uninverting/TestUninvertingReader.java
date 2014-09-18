package org.apache.lucene.uninverting;

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
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestUninvertingReader extends LuceneTestCase {
  
  public void testSortedSetInteger() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new IntField("foo", 5, Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new IntField("foo", 5, Field.Store.NO));
    doc.add(new IntField("foo", -3, Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
                         Collections.singletonMap("foo", Type.SORTED_SET_INTEGER));
    AtomicReader ar = ir.leaves().get(0).reader();
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, NumericUtils.prefixCodedToInt(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, NumericUtils.prefixCodedToInt(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetFloat() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new IntField("foo", Float.floatToRawIntBits(5f), Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new IntField("foo", Float.floatToRawIntBits(5f), Field.Store.NO));
    doc.add(new IntField("foo", Float.floatToRawIntBits(-3f), Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
                         Collections.singletonMap("foo", Type.SORTED_SET_FLOAT));
    AtomicReader ar = ir.leaves().get(0).reader();
    
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(Float.floatToRawIntBits(-3f), NumericUtils.prefixCodedToInt(value));
    
    value = v.lookupOrd(1);
    assertEquals(Float.floatToRawIntBits(5f), NumericUtils.prefixCodedToInt(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetLong() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new LongField("foo", 5, Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LongField("foo", 5, Field.Store.NO));
    doc.add(new LongField("foo", -3, Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
        Collections.singletonMap("foo", Type.SORTED_SET_LONG));
    AtomicReader ar = ir.leaves().get(0).reader();
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, NumericUtils.prefixCodedToLong(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, NumericUtils.prefixCodedToLong(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetDouble() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new LongField("foo", Double.doubleToRawLongBits(5d), Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LongField("foo", Double.doubleToRawLongBits(5d), Field.Store.NO));
    doc.add(new LongField("foo", Double.doubleToRawLongBits(-3d), Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
        Collections.singletonMap("foo", Type.SORTED_SET_DOUBLE));
    AtomicReader ar = ir.leaves().get(0).reader();
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(Double.doubleToRawLongBits(-3d), NumericUtils.prefixCodedToLong(value));
    
    value = v.lookupOrd(1);
    assertEquals(Double.doubleToRawLongBits(5d), NumericUtils.prefixCodedToLong(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
}
