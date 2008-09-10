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
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.IOException;
import org.apache.lucene.search.Similarity;

public class TestSegmentTermDocs extends LuceneTestCase {
  private Document testDoc = new Document();
  private Directory dir = new RAMDirectory();
  private SegmentInfo info;

  public TestSegmentTermDocs(String s) {
    super(s);
  }

  protected void setUp() throws Exception {
    super.setUp();
    DocHelper.setupDoc(testDoc);
    info = DocHelper.writeDoc(dir, testDoc);
  }

  public void test() {
    assertTrue(dir != null);
  }
  
  public void testTermDocs() throws IOException {
    testTermDocs(1);
  }

  public void testTermDocs(int indexDivisor) throws IOException {
    //After adding the document, we should be able to read it back in
    SegmentReader reader = SegmentReader.get(info);
    reader.setTermInfosIndexDivisor(indexDivisor);
    assertTrue(reader != null);
    SegmentTermDocs segTermDocs = new SegmentTermDocs(reader);
    assertTrue(segTermDocs != null);
    segTermDocs.seek(new Term(DocHelper.TEXT_FIELD_2_KEY, "field"));
    if (segTermDocs.next() == true)
    {
      int docId = segTermDocs.doc();
      assertTrue(docId == 0);
      int freq = segTermDocs.freq();
      assertTrue(freq == 3);  
    }
    reader.close();
  }  
  
  public void testBadSeek() throws IOException {
    testBadSeek(1);
  }

  public void testBadSeek(int indexDivisor) throws IOException {
    {
      //After adding the document, we should be able to read it back in
      SegmentReader reader = SegmentReader.get(info);
      reader.setTermInfosIndexDivisor(indexDivisor);
      assertTrue(reader != null);
      SegmentTermDocs segTermDocs = new SegmentTermDocs(reader);
      assertTrue(segTermDocs != null);
      segTermDocs.seek(new Term("textField2", "bad"));
      assertTrue(segTermDocs.next() == false);
      reader.close();
    }
    {
      //After adding the document, we should be able to read it back in
      SegmentReader reader = SegmentReader.get(info);
      reader.setTermInfosIndexDivisor(indexDivisor);
      assertTrue(reader != null);
      SegmentTermDocs segTermDocs = new SegmentTermDocs(reader);
      assertTrue(segTermDocs != null);
      segTermDocs.seek(new Term("junk", "bad"));
      assertTrue(segTermDocs.next() == false);
      reader.close();
    }
  }
  
  public void testSkipTo() throws IOException {
    testSkipTo(1);
  }

  public void testSkipTo(int indexDivisor) throws IOException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, 
                                         IndexWriter.MaxFieldLength.LIMITED);
    
    Term ta = new Term("content","aaa");
    for(int i = 0; i < 10; i++)
      addDoc(writer, "aaa aaa aaa aaa");
      
    Term tb = new Term("content","bbb");
    for(int i = 0; i < 16; i++)
      addDoc(writer, "bbb bbb bbb bbb");
      
    Term tc = new Term("content","ccc");
    for(int i = 0; i < 50; i++)
      addDoc(writer, "ccc ccc ccc ccc");
      
    // assure that we deal with a single segment  
    writer.optimize();
    writer.close();
    
    IndexReader reader = IndexReader.open(dir);
    reader.setTermInfosIndexDivisor(indexDivisor);
    assertEquals(indexDivisor, reader.getTermInfosIndexDivisor());

    TermDocs tdocs = reader.termDocs();
    
    // without optimization (assumption skipInterval == 16)
    
    // with next
    tdocs.seek(ta);
    assertTrue(tdocs.next());
    assertEquals(0, tdocs.doc());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.next());
    assertEquals(1, tdocs.doc());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.skipTo(0));
    assertEquals(2, tdocs.doc());
    assertTrue(tdocs.skipTo(4));
    assertEquals(4, tdocs.doc());
    assertTrue(tdocs.skipTo(9));
    assertEquals(9, tdocs.doc());
    assertFalse(tdocs.skipTo(10));
    
    // without next
    tdocs.seek(ta);
    assertTrue(tdocs.skipTo(0));
    assertEquals(0, tdocs.doc());
    assertTrue(tdocs.skipTo(4));
    assertEquals(4, tdocs.doc());
    assertTrue(tdocs.skipTo(9));
    assertEquals(9, tdocs.doc());
    assertFalse(tdocs.skipTo(10));
    
    // exactly skipInterval documents and therefore with optimization
    
    // with next
    tdocs.seek(tb);
    assertTrue(tdocs.next());
    assertEquals(10, tdocs.doc());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.next());
    assertEquals(11, tdocs.doc());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.skipTo(5));
    assertEquals(12, tdocs.doc());
    assertTrue(tdocs.skipTo(15));
    assertEquals(15, tdocs.doc());
    assertTrue(tdocs.skipTo(24));
    assertEquals(24, tdocs.doc());
    assertTrue(tdocs.skipTo(25));
    assertEquals(25, tdocs.doc());
    assertFalse(tdocs.skipTo(26));
    
    // without next
    tdocs.seek(tb);
    assertTrue(tdocs.skipTo(5));
    assertEquals(10, tdocs.doc());
    assertTrue(tdocs.skipTo(15));
    assertEquals(15, tdocs.doc());
    assertTrue(tdocs.skipTo(24));
    assertEquals(24, tdocs.doc());
    assertTrue(tdocs.skipTo(25));
    assertEquals(25, tdocs.doc());
    assertFalse(tdocs.skipTo(26));
    
    // much more than skipInterval documents and therefore with optimization
    
    // with next
    tdocs.seek(tc);
    assertTrue(tdocs.next());
    assertEquals(26, tdocs.doc());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.next());
    assertEquals(27, tdocs.doc());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.skipTo(5));
    assertEquals(28, tdocs.doc());
    assertTrue(tdocs.skipTo(40));
    assertEquals(40, tdocs.doc());
    assertTrue(tdocs.skipTo(57));
    assertEquals(57, tdocs.doc());
    assertTrue(tdocs.skipTo(74));
    assertEquals(74, tdocs.doc());
    assertTrue(tdocs.skipTo(75));
    assertEquals(75, tdocs.doc());
    assertFalse(tdocs.skipTo(76));
    
    //without next
    tdocs.seek(tc);
    assertTrue(tdocs.skipTo(5));
    assertEquals(26, tdocs.doc());
    assertTrue(tdocs.skipTo(40));
    assertEquals(40, tdocs.doc());
    assertTrue(tdocs.skipTo(57));
    assertEquals(57, tdocs.doc());
    assertTrue(tdocs.skipTo(74));
    assertEquals(74, tdocs.doc());
    assertTrue(tdocs.skipTo(75));
    assertEquals(75, tdocs.doc());
    assertFalse(tdocs.skipTo(76));
    
    tdocs.close();
    reader.close();
    dir.close();
  }
  
  public void testIndexDivisor() throws IOException {
    dir = new MockRAMDirectory();
    testDoc = new Document();
    DocHelper.setupDoc(testDoc);
    DocHelper.writeDoc(dir, testDoc);
    testTermDocs(2);
    testBadSeek(2);
    testSkipTo(2);
  }
  
  public void testIndexDivisorAfterLoad() throws IOException {
    dir = new MockRAMDirectory();
    testDoc = new Document();
    DocHelper.setupDoc(testDoc);
    SegmentInfo si = DocHelper.writeDoc(dir, testDoc);
    SegmentReader reader = SegmentReader.get(si);
    assertEquals(1, reader.docFreq(new Term("keyField", "Keyword")));
    try {
      reader.setTermInfosIndexDivisor(2);
      fail("did not hit IllegalStateException exception");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  private void addDoc(IndexWriter writer, String value) throws IOException
  {
      Document doc = new Document();
      doc.add(new Field("content", value, Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
  }
}
