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
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldsUpdate.Operation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestFieldUpdates extends LuceneTestCase {
  private Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }
  
  public void testUpdateDocumentBeforeCommit() throws Exception {
    createAndAssertSegment(false);
  }
  
  public void testUpdateDocumentAfterCommit() throws Exception {
    createAndAssertSegment(true);
  }
  
  private void createAndAssertSegment(boolean interCommit) throws IOException {
    // added doc contains at least the first field, updated field at least the
    // last field, other fields split in the middle randomly
    int numFields = DocHelper.numFields();
    int cutoff = random().nextInt(numFields - 2);
    createSegment(cutoff + 1, interCommit);
    assertSegment();
  }
  
  private void createSegment(int cutoff, boolean interCommit)
      throws IOException {
    // add base document
    Document testDoc = new Document();
    DocHelper.setupDoc(testDoc, 0, cutoff);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    writer.addDocument(testDoc);
    if (interCommit) {
      writer.commit();
    }
    
    // add updates to base document
    Document updateDoc = new Document();
    DocHelper.setupDoc(updateDoc, cutoff, DocHelper.numFields());
    writer.updateFields(Operation.ADD_FIELDS, new Term(
        DocHelper.TEXT_FIELD_1_KEY, DocHelper.FIELD_1_TEXT.split(" ")[0]),
        updateDoc);
    writer.close();
  }
  
  private void assertSegment() throws IOException {
    // After adding the document, we should be able to read it back in
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    List<AtomicReaderContext> leaves = directoryReader.leaves();
    assertEquals("wrong number of atomic readers", 1, leaves.size());
    AtomicReaderContext atomicReaderContext = leaves.get(0);
    AtomicReader reader = atomicReaderContext.reader();
    assertTrue(reader != null);
    StoredDocument doc = reader.document(0);
    assertTrue(doc != null);
    
    // System.out.println("Document: " + doc);
    StorableField[] fields = doc.getFields("textField2");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_2_TEXT));
    assertTrue(fields[0].fieldType().storeTermVectors());
    
    fields = doc.getFields("textField1");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_1_TEXT));
    assertFalse(fields[0].fieldType().storeTermVectors());
    
    fields = doc.getFields("keyField");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.KEYWORD_TEXT));
    
    fields = doc.getFields(DocHelper.NO_NORMS_KEY);
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.NO_NORMS_TEXT));
    
    fields = doc.getFields(DocHelper.TEXT_FIELD_3_KEY);
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_3_TEXT));
    
    // test that the norms are not present in the segment if
    // omitNorms is true
    for (FieldInfo fi : reader.getFieldInfos()) {
      if (fi.isIndexed()) {
        assertTrue(fi.omitsNorms() == (reader.normValues(fi.name) == null));
      }
    }
    reader.close();
  }
  
  public void testSegmentWithDeletion() throws IOException {
    // add base document
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document testDoc = new Document();
    DocHelper.setupDoc(testDoc, 0, 5);
    writer.addDocument(testDoc);
    testDoc = new Document();
    DocHelper.setupDoc(testDoc, 5, DocHelper.numFields());
    writer.addDocument(testDoc);
    writer.commit();
    
    writer.deleteDocuments(new Term(DocHelper.TEXT_FIELD_1_KEY,
        DocHelper.FIELD_1_TEXT.split(" ")[0]));
    writer.close();
    
    assertSegment();
  }
  
}
