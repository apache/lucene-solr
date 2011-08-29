package org.apache.lucene.index;

/**
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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorVisitor;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestContribIndexReader extends LuceneTestCase {
  private Document getDocument(IndexReader ir, int docID, FieldSelector selector)  throws IOException {
    final FieldSelectorVisitor visitor = new FieldSelectorVisitor(selector);
    ir.document(docID, visitor);
    return visitor.getDocument();
  }

  static void addDoc(IndexWriter writer, String value) throws IOException {
    Document doc = new Document();
    doc.add(newField("content", value, TextField.TYPE_UNSTORED));
    writer.addDocument(doc);
  }

  static void addDocumentWithFields(IndexWriter writer) throws IOException {
    Document doc = new Document();
        
    FieldType customType3 = new FieldType();
    customType3.setStored(true);
    doc.add(newField("keyword", "test1", StringField.TYPE_STORED));
    doc.add(newField("text", "test1", TextField.TYPE_STORED));
    doc.add(newField("unindexed", "test1", customType3));
    doc.add(new TextField("unstored","test1"));
    writer.addDocument(doc);
  }


  static void addDocumentWithDifferentFields(IndexWriter writer) throws IOException {
    Document doc = new Document();
      
    FieldType customType3 = new FieldType();
    customType3.setStored(true);
    doc.add(newField("keyword2", "test1", StringField.TYPE_STORED));
    doc.add(newField("text2", "test1", TextField.TYPE_STORED));
    doc.add(newField("unindexed2", "test1", customType3));
    doc.add(new TextField("unstored2","test1"));
    writer.addDocument(doc);
  }

  static void addDocumentWithTermVectorFields(IndexWriter writer) throws IOException {
    Document doc = new Document();
    FieldType customType5 = new FieldType(TextField.TYPE_STORED);
    customType5.setStoreTermVectors(true);
    FieldType customType6 = new FieldType(TextField.TYPE_STORED);
    customType6.setStoreTermVectors(true);
    customType6.setStoreTermVectorOffsets(true);
    FieldType customType7 = new FieldType(TextField.TYPE_STORED);
    customType7.setStoreTermVectors(true);
    customType7.setStoreTermVectorPositions(true);
    FieldType customType8 = new FieldType(TextField.TYPE_STORED);
    customType8.setStoreTermVectors(true);
    customType8.setStoreTermVectorOffsets(true);
    customType8.setStoreTermVectorPositions(true);
    doc.add(newField("tvnot","tvnot",TextField.TYPE_STORED));
    doc.add(newField("termvector","termvector",customType5));
    doc.add(newField("tvoffset","tvoffset", customType6));
    doc.add(newField("tvposition","tvposition", customType7));
    doc.add(newField("tvpositionoffset","tvpositionoffset", customType8));
        
    writer.addDocument(doc);
  }

  public void testBinaryFields() throws IOException {
    Directory dir = newDirectory();
    byte[] bin = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
        
    for (int i = 0; i < 10; i++) {
      addDoc(writer, "document number " + (i + 1));
      addDocumentWithFields(writer);
      addDocumentWithDifferentFields(writer);
      addDocumentWithTermVectorFields(writer);
    }
    writer.close();
    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    doc.add(new BinaryField("bin1", bin));
    doc.add(new TextField("junk", "junk text"));
    writer.addDocument(doc);
    writer.close();
    IndexReader reader = IndexReader.open(dir, false);
    Document doc2 = reader.document(reader.maxDoc() - 1);
    IndexableField[] fields = doc2.getFields("bin1");
    assertNotNull(fields);
    assertEquals(1, fields.length);
    Field b1 = (Field) fields[0];
    assertTrue(b1.isBinary());
    BytesRef bytesRef = b1.binaryValue();
    assertEquals(bin.length, bytesRef.length);
    for (int i = 0; i < bin.length; i++) {
      assertEquals(bin[i], bytesRef.bytes[i + bytesRef.offset]);
    }
    Set<String> lazyFields = new HashSet<String>();
    lazyFields.add("bin1");
    FieldSelector sel = new SetBasedFieldSelector(new HashSet<String>(), lazyFields);
    doc2 = getDocument(reader, reader.maxDoc() - 1, sel);
    fields = doc2.getFields("bin1");
    assertNotNull(fields);
    assertEquals(1, fields.length);
    IndexableField fb1 = fields[0];
    assertTrue(fb1.binaryValue()!=null);
    bytesRef = fb1.binaryValue();
    assertEquals(bin.length, bytesRef.bytes.length);
    assertEquals(bin.length, bytesRef.length);
    for (int i = 0; i < bin.length; i++) {
      assertEquals(bin[i], bytesRef.bytes[i + bytesRef.offset]);
    }
    reader.close();
    // force optimize


    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND).setMergePolicy(newLogMergePolicy()));
    writer.optimize();
    writer.close();
    reader = IndexReader.open(dir, false);
    doc2 = reader.document(reader.maxDoc() - 1);
    fields = doc2.getFields("bin1");
    assertNotNull(fields);
    assertEquals(1, fields.length);
    b1 = (Field) fields[0];
    assertTrue(b1.isBinary());
    bytesRef = b1.binaryValue();
    assertEquals(bin.length, bytesRef.length);
    for (int i = 0; i < bin.length; i++) {
      assertEquals(bin[i], bytesRef.bytes[i + bytesRef.offset]);
    }
    reader.close();
    dir.close();
  }
}
