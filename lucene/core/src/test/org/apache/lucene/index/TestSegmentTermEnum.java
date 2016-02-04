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

import org.apache.lucene.document.Field;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;


public class TestSegmentTermEnum extends LuceneTestCase {
  
  Directory dir;
  
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

  public void testTermEnum() throws IOException {
    IndexWriter writer = null;

    writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    // ADD 100 documents with term : aaa
    // add 100 documents with terms: aaa bbb
    // Therefore, term 'aaa' has document frequency of 200 and term 'bbb' 100
    for (int i = 0; i < 100; i++) {
      addDoc(writer, "aaa");
      addDoc(writer, "aaa bbb");
    }

    writer.close();

    // verify document frequency of terms in an multi segment index
    verifyDocFreq();

    // merge segments
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    // verify document frequency of terms in a single segment index
    verifyDocFreq();
  }

  public void testPrevTermAtEnd() throws IOException
  {
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                .setCodec(TestUtil.alwaysPostingsFormat(TestUtil.getDefaultPostingsFormat())));
    addDoc(writer, "aaa bbb");
    writer.close();
    SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(dir));
    TermsEnum terms = reader.fields().terms("content").iterator();
    assertNotNull(terms.next());
    assertEquals("aaa", terms.term().utf8ToString());
    assertNotNull(terms.next());
    long ordB;
    try {
      ordB = terms.ord();
    } catch (UnsupportedOperationException uoe) {
      // ok -- codec is not required to support ord
      reader.close();
      return;
    }
    assertEquals("bbb", terms.term().utf8ToString());
    assertNull(terms.next());

    terms.seekExact(ordB);
    assertEquals("bbb", terms.term().utf8ToString());
    reader.close();
  }

  private void verifyDocFreq()
      throws IOException
  {
      IndexReader reader = DirectoryReader.open(dir);
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
    termEnum.seekCeil(new BytesRef("aaa"));
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
    doc.add(newTextField("content", value, Field.Store.NO));
    writer.addDocument(doc);
  }
}
