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

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class TestDirectoryReader extends LuceneTestCase {
  protected Directory dir;
  private Document doc1;
  private Document doc2;
  protected SegmentReader [] readers = new SegmentReader[2];
  protected SegmentInfos sis;
  
  
  public TestDirectoryReader(String s) {
    super(s);
  }

  protected void setUp() throws Exception {
    super.setUp();
    dir = new RAMDirectory();
    doc1 = new Document();
    doc2 = new Document();
    DocHelper.setupDoc(doc1);
    DocHelper.setupDoc(doc2);
    DocHelper.writeDoc(dir, doc1);
    DocHelper.writeDoc(dir, doc2);
    sis = new SegmentInfos();
    sis.read(dir);
  }

  protected IndexReader openReader() throws IOException {
    IndexReader reader;
    reader = IndexReader.open(dir);
    assertTrue(reader instanceof DirectoryReader);

    assertTrue(dir != null);
    assertTrue(sis != null);
    assertTrue(reader != null);
    
    return reader;
  }

  public void test() throws Exception {
    setUp();
    doTestDocument();
    doTestUndeleteAll();
  }    

  public void doTestDocument() throws IOException {
    sis.read(dir);
    IndexReader reader = openReader();
    assertTrue(reader != null);
    Document newDoc1 = reader.document(0);
    assertTrue(newDoc1 != null);
    assertTrue(DocHelper.numFields(newDoc1) == DocHelper.numFields(doc1) - DocHelper.unstored.size());
    Document newDoc2 = reader.document(1);
    assertTrue(newDoc2 != null);
    assertTrue(DocHelper.numFields(newDoc2) == DocHelper.numFields(doc2) - DocHelper.unstored.size());
    TermFreqVector vector = reader.getTermFreqVector(0, DocHelper.TEXT_FIELD_2_KEY);
    assertTrue(vector != null);
    TestSegmentReader.checkNorms(reader);
  }

  public void doTestUndeleteAll() throws IOException {
    sis.read(dir);
    IndexReader reader = openReader();
    assertTrue(reader != null);
    assertEquals( 2, reader.numDocs() );
    reader.deleteDocument(0);
    assertEquals( 1, reader.numDocs() );
    reader.undeleteAll();
    assertEquals( 2, reader.numDocs() );

    // Ensure undeleteAll survives commit/close/reopen:
    reader.commit();
    reader.close();

    if (reader instanceof MultiReader)
      // MultiReader does not "own" the directory so it does
      // not write the changes to sis on commit:
      sis.commit(dir);

    sis.read(dir);
    reader = openReader();
    assertEquals( 2, reader.numDocs() );

    reader.deleteDocument(0);
    assertEquals( 1, reader.numDocs() );
    reader.commit();
    reader.close();
    if (reader instanceof MultiReader)
      // MultiReader does not "own" the directory so it does
      // not write the changes to sis on commit:
      sis.commit(dir);
    sis.read(dir);
    reader = openReader();
    assertEquals( 1, reader.numDocs() );
  }
        
  
  public void _testTermVectors() {
    MultiReader reader = new MultiReader(readers);
    assertTrue(reader != null);
  }
  

  public void testIsCurrent() throws IOException {
    RAMDirectory ramDir1=new RAMDirectory();
    addDoc(ramDir1, "test foo", true);
    RAMDirectory ramDir2=new RAMDirectory();
    addDoc(ramDir2, "test blah", true);
    IndexReader[] readers = new IndexReader[]{IndexReader.open(ramDir1), IndexReader.open(ramDir2)};
    MultiReader mr = new MultiReader(readers);
    assertTrue(mr.isCurrent());   // just opened, must be current
    addDoc(ramDir1, "more text", false);
    assertFalse(mr.isCurrent());   // has been modified, not current anymore
    addDoc(ramDir2, "even more text", false);
    assertFalse(mr.isCurrent());   // has been modified even more, not current anymore
    try {
      mr.getVersion();
      fail();
    } catch (UnsupportedOperationException e) {
      // expected exception
    }
    mr.close();
  }

  public void testMultiTermDocs() throws IOException {
    RAMDirectory ramDir1=new RAMDirectory();
    addDoc(ramDir1, "test foo", true);
    RAMDirectory ramDir2=new RAMDirectory();
    addDoc(ramDir2, "test blah", true);
    RAMDirectory ramDir3=new RAMDirectory();
    addDoc(ramDir3, "test wow", true);

    IndexReader[] readers1 = new IndexReader[]{IndexReader.open(ramDir1), IndexReader.open(ramDir3)};
    IndexReader[] readers2 = new IndexReader[]{IndexReader.open(ramDir1), IndexReader.open(ramDir2), IndexReader.open(ramDir3)};
    MultiReader mr2 = new MultiReader(readers1);
    MultiReader mr3 = new MultiReader(readers2);

    // test mixing up TermDocs and TermEnums from different readers.
    TermDocs td2 = mr2.termDocs();
    TermEnum te3 = mr3.terms(new Term("body","wow"));
    td2.seek(te3);
    int ret = 0;

    // This should blow up if we forget to check that the TermEnum is from the same
    // reader as the TermDocs.
    while (td2.next()) ret += td2.doc();
    td2.close();
    te3.close();

    // really a dummy assert to ensure that we got some docs and to ensure that
    // nothing is optimized out.
    assertTrue(ret > 0);
  }

  public void testAllTermDocs() throws IOException {
    IndexReader reader = openReader();
    int NUM_DOCS = 2;
    TermDocs td = reader.termDocs(null);
    for(int i=0;i<NUM_DOCS;i++) {
      assertTrue(td.next());
      assertEquals(i, td.doc());
      assertEquals(1, td.freq());
    }
    td.close();
    reader.close();
  }

  private void addDoc(RAMDirectory ramDir1, String s, boolean create) throws IOException {
    IndexWriter iw = new IndexWriter(ramDir1, new StandardAnalyzer(), create, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("body", s, Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
    iw.close();
  }
}
