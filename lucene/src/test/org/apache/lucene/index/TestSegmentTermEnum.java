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
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;


public class TestSegmentTermEnum extends LuceneTestCase {
  
  Directory dir;
  Random random;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    random = newRandom();
    dir = newDirectory(random);
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  public void testTermEnum() throws IOException {
    IndexWriter writer = null;

    writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));

    // ADD 100 documents with term : aaa
    // add 100 documents with terms: aaa bbb
    // Therefore, term 'aaa' has document frequency of 200 and term 'bbb' 100
    for (int i = 0; i < 100; i++) {
      addDoc(writer, "aaa");
      addDoc(writer, "aaa bbb");
    }

    writer.close();

    // verify document frequency of terms in an unoptimized index
    verifyDocFreq();

    // merge segments by optimizing the index
    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
    writer.optimize();
    writer.close();

    // verify document frequency of terms in an optimized index
    verifyDocFreq();
  }

  public void testPrevTermAtEnd() throws IOException
  {
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setCodecProvider(_TestUtil.alwaysCodec("Standard")));
    addDoc(writer, "aaa bbb");
    writer.close();
    SegmentReader reader = SegmentReader.getOnlySegmentReader(dir);
    TermsEnum terms = reader.fields().terms("content").iterator();
    assertNotNull(terms.next());
    assertEquals("aaa", terms.term().utf8ToString());
    assertNotNull(terms.next());
    long ordB = terms.ord();
    assertEquals("bbb", terms.term().utf8ToString());
    assertNull(terms.next());

    assertEquals(TermsEnum.SeekStatus.FOUND, terms.seek(ordB));
    assertEquals("bbb", terms.term().utf8ToString());
    reader.close();
  }

  private void verifyDocFreq()
      throws IOException
  {
      IndexReader reader = IndexReader.open(dir, true);
      TermsEnum termEnum = MultiFields.getTerms(reader, "content").iterator();

    // create enumeration of all terms
    // go to the first term (aaa)
    termEnum.next();
    // assert that term is 'aaa'
    assertEquals("aaa", termEnum.term().utf8ToString());
    assertEquals(200, termEnum.docFreq());
    // go to the second term (bbb)
    termEnum.next();
    // assert that term is 'bbb'
    assertEquals("bbb", termEnum.term().utf8ToString());
    assertEquals(100, termEnum.docFreq());


    // create enumeration of terms after term 'aaa',
    // including 'aaa'
    termEnum.seek(new BytesRef("aaa"));
    // assert that term is 'aaa'
    assertEquals("aaa", termEnum.term().utf8ToString());
    assertEquals(200, termEnum.docFreq());
    // go to term 'bbb'
    termEnum.next();
    // assert that term is 'bbb'
    assertEquals("bbb", termEnum.term().utf8ToString());
    assertEquals(100, termEnum.docFreq());
    reader.close();
  }

  private void addDoc(IndexWriter writer, String value) throws IOException
  {
    Document doc = new Document();
    doc.add(new Field("content", value, Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
  }
}
