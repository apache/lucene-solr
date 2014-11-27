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

import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestUninvertingReader extends LuceneTestCase {

  // nocommit also make back-compat variants of these
  public void testSortedSetInteger() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.disableSorting("foo");
    fieldTypes.setMultiValued("foo");

    Document2 doc = iw.newDocument();
    doc.addInt("foo", 5);
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addInt("foo", 5);
    doc.addInt("foo", -3);
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
                         Collections.singletonMap("foo", Type.SORTED_SET_INTEGER));
    LeafReader ar = ir.leaves().get(0).reader();
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
    assertEquals(-3, Document2.bytesToInt(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, Document2.bytesToInt(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetFloat() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.disableSorting("foo");
    fieldTypes.setMultiValued("foo");
    
    Document2 doc = iw.newDocument();
    doc.addFloat("foo", 5f);
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addFloat("foo", 5f);
    doc.addFloat("foo", -3f);
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
                         Collections.singletonMap("foo", Type.SORTED_SET_FLOAT));
    LeafReader ar = ir.leaves().get(0).reader();
    
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
    assertEquals(-3f, Document2.bytesToFloat(value), 0.0f);
    
    value = v.lookupOrd(1);
    assertEquals(5f, Document2.bytesToFloat(value), 0.0f);
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetLong() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.disableSorting("foo");
    fieldTypes.setMultiValued("foo");
    
    Document2 doc = iw.newDocument();
    doc.addLong("foo", 5);
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addLong("foo", 5);
    doc.addLong("foo", -3);
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
        Collections.singletonMap("foo", Type.SORTED_SET_LONG));
    LeafReader ar = ir.leaves().get(0).reader();
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
    assertEquals(-3, Document2.bytesToLong(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, Document2.bytesToLong(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetDouble() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.disableSorting("foo");
    fieldTypes.setMultiValued("foo");
    
    Document2 doc = iw.newDocument();
    doc.addDouble("foo", 5d);
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addDouble("foo", 5d);
    doc.addDouble("foo", -3d);
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
        Collections.singletonMap("foo", Type.SORTED_SET_DOUBLE));
    LeafReader ar = ir.leaves().get(0).reader();
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
    assertEquals(-3d, Document2.bytesToDouble(value), 0.0);
    
    value = v.lookupOrd(1);
    assertEquals(5d, Document2.bytesToDouble(value), 0.0);
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
}
