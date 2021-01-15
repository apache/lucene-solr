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

public class TestParallelTermEnum extends LuceneTestCase {
  private LeafReader ir1;
  private LeafReader ir2;
  private Directory rd1;
  private Directory rd2;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Document doc;
    rd1 = newDirectory();
    IndexWriter iw1 = new IndexWriter(rd1, newIndexWriterConfig(new MockAnalyzer(random())));

    doc = new Document();
    doc.add(newTextField("field1", "the quick brown fox jumps", Field.Store.YES));
    doc.add(newTextField("field2", "the quick brown fox jumps", Field.Store.YES));
    iw1.addDocument(doc);

    iw1.close();
    rd2 = newDirectory();
    IndexWriter iw2 = new IndexWriter(rd2, newIndexWriterConfig(new MockAnalyzer(random())));

    doc = new Document();
    doc.add(newTextField("field1", "the fox jumps over the lazy dog", Field.Store.YES));
    doc.add(newTextField("field3", "the fox jumps over the lazy dog", Field.Store.YES));
    iw2.addDocument(doc);

    iw2.close();

    this.ir1 = getOnlyLeafReader(DirectoryReader.open(rd1));
    this.ir2 = getOnlyLeafReader(DirectoryReader.open(rd2));
  }

  @Override
  public void tearDown() throws Exception {
    ir1.close();
    ir2.close();
    rd1.close();
    rd2.close();
    super.tearDown();
  }

  private void checkTerms(Terms terms, String... termsList) throws IOException {
    assertNotNull(terms);
    final TermsEnum te = terms.iterator();

    for (String t : termsList) {
      BytesRef b = te.next();
      assertNotNull(b);
      assertEquals(t, b.utf8ToString());
      PostingsEnum td = TestUtil.docs(random(), te, null, PostingsEnum.NONE);
      assertTrue(td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(0, td.docID());
      assertEquals(td.nextDoc(), DocIdSetIterator.NO_MORE_DOCS);
    }
    assertNull(te.next());
  }

  public void test1() throws IOException {
    ParallelLeafReader pr = new ParallelLeafReader(ir1, ir2);

    assertEquals(3, pr.getFieldInfos().size());

    checkTerms(pr.terms("field1"), "brown", "fox", "jumps", "quick", "the");
    checkTerms(pr.terms("field2"), "brown", "fox", "jumps", "quick", "the");
    checkTerms(pr.terms("field3"), "dog", "fox", "jumps", "lazy", "over", "the");
  }
}
