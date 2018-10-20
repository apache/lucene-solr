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


import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestSegmentTermDocs extends LuceneTestCase {
  private Document testDoc = new Document();
  private Directory dir;
  private SegmentCommitInfo info;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    DocHelper.setupDoc(testDoc);
    info = DocHelper.writeDoc(random(), dir, testDoc);
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  public void test() {
    assertTrue(dir != null);
  }

  public void testTermDocs() throws IOException {
    //After adding the document, we should be able to read it back in
    SegmentReader reader = new SegmentReader(info, Version.LATEST.major, newIOContext(random()));
    assertTrue(reader != null);

    TermsEnum terms = reader.terms(DocHelper.TEXT_FIELD_2_KEY).iterator();
    terms.seekCeil(new BytesRef("field"));
    PostingsEnum termDocs = TestUtil.docs(random(), terms, null, PostingsEnum.FREQS);
    if (termDocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS)    {
      int docId = termDocs.docID();
      assertTrue(docId == 0);
      int freq = termDocs.freq();
      assertTrue(freq == 3);  
    }
    reader.close();
  }  
  
  public void testBadSeek() throws IOException {
    {
      //After adding the document, we should be able to read it back in
      SegmentReader reader = new SegmentReader(info, Version.LATEST.major, newIOContext(random()));
      assertTrue(reader != null);
      PostingsEnum termDocs = TestUtil.docs(random(), reader,
          "textField2",
          new BytesRef("bad"),
          null,
          0);

      assertNull(termDocs);
      reader.close();
    }
    {
      //After adding the document, we should be able to read it back in
      SegmentReader reader = new SegmentReader(info, Version.LATEST.major, newIOContext(random()));
      assertTrue(reader != null);
      PostingsEnum termDocs = TestUtil.docs(random(), reader,
          "junk",
          new BytesRef("bad"),
          null,
          0);
      assertNull(termDocs);
      reader.close();
    }
  }
  
  public void testSkipTo() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMergePolicy(newLogMergePolicy()));
    
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
    writer.forceMerge(1);
    writer.close();
    
    IndexReader reader = DirectoryReader.open(dir);

    PostingsEnum tdocs = TestUtil.docs(random(), reader,
        ta.field(),
        new BytesRef(ta.text()),
        null,
        PostingsEnum.FREQS);
    
    // without optimization (assumption skipInterval == 16)
    
    // with next
    assertTrue(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(0, tdocs.docID());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, tdocs.docID());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.advance(2) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(2, tdocs.docID());
    assertTrue(tdocs.advance(4) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(4, tdocs.docID());
    assertTrue(tdocs.advance(9) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(9, tdocs.docID());
    assertFalse(tdocs.advance(10) != DocIdSetIterator.NO_MORE_DOCS);
    
    // without next
    tdocs = TestUtil.docs(random(), reader,
        ta.field(),
        new BytesRef(ta.text()),
        null,
        0);
    
    assertTrue(tdocs.advance(0) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(0, tdocs.docID());
    assertTrue(tdocs.advance(4) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(4, tdocs.docID());
    assertTrue(tdocs.advance(9) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(9, tdocs.docID());
    assertFalse(tdocs.advance(10) != DocIdSetIterator.NO_MORE_DOCS);
    
    // exactly skipInterval documents and therefore with optimization
    
    // with next
    tdocs = TestUtil.docs(random(), reader,
        tb.field(),
        new BytesRef(tb.text()),
        null,
        PostingsEnum.FREQS);

    assertTrue(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(10, tdocs.docID());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(11, tdocs.docID());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.advance(12) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(12, tdocs.docID());
    assertTrue(tdocs.advance(15) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(15, tdocs.docID());
    assertTrue(tdocs.advance(24) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(24, tdocs.docID());
    assertTrue(tdocs.advance(25) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(25, tdocs.docID());
    assertFalse(tdocs.advance(26) != DocIdSetIterator.NO_MORE_DOCS);
    
    // without next
    tdocs = TestUtil.docs(random(), reader,
        tb.field(),
        new BytesRef(tb.text()),
        null,
        PostingsEnum.FREQS);
    
    assertTrue(tdocs.advance(5) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(10, tdocs.docID());
    assertTrue(tdocs.advance(15) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(15, tdocs.docID());
    assertTrue(tdocs.advance(24) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(24, tdocs.docID());
    assertTrue(tdocs.advance(25) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(25, tdocs.docID());
    assertFalse(tdocs.advance(26) != DocIdSetIterator.NO_MORE_DOCS);
    
    // much more than skipInterval documents and therefore with optimization
    
    // with next
    tdocs = TestUtil.docs(random(), reader,
        tc.field(),
        new BytesRef(tc.text()),
        null,
        PostingsEnum.FREQS);

    assertTrue(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(26, tdocs.docID());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(27, tdocs.docID());
    assertEquals(4, tdocs.freq());
    assertTrue(tdocs.advance(28) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(28, tdocs.docID());
    assertTrue(tdocs.advance(40) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(40, tdocs.docID());
    assertTrue(tdocs.advance(57) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(57, tdocs.docID());
    assertTrue(tdocs.advance(74) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(74, tdocs.docID());
    assertTrue(tdocs.advance(75) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(75, tdocs.docID());
    assertFalse(tdocs.advance(76) != DocIdSetIterator.NO_MORE_DOCS);
    
    //without next
    tdocs = TestUtil.docs(random(), reader,
        tc.field(),
        new BytesRef(tc.text()),
        null,
        0);
    assertTrue(tdocs.advance(5) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(26, tdocs.docID());
    assertTrue(tdocs.advance(40) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(40, tdocs.docID());
    assertTrue(tdocs.advance(57) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(57, tdocs.docID());
    assertTrue(tdocs.advance(74) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(74, tdocs.docID());
    assertTrue(tdocs.advance(75) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(75, tdocs.docID());
    assertFalse(tdocs.advance(76) != DocIdSetIterator.NO_MORE_DOCS);
    
    reader.close();
    dir.close();
  }


  private void addDoc(IndexWriter writer, String value) throws IOException
  {
      Document doc = new Document();
      doc.add(newTextField("content", value, Field.Store.NO));
      writer.addDocument(doc);
  }
}
