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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestFlex extends LuceneTestCase {

  // Test non-flex API emulated on flex index
  public void testNonFlex() throws Exception {
    Directory d = newDirectory();

    final int DOC_COUNT = 177;

    IndexWriter w = new IndexWriter(
        d,
        new IndexWriterConfig(new MockAnalyzer(random())).
            setMaxBufferedDocs(7).setMergePolicy(newLogMergePolicy())
    );

    for(int iter=0;iter<2;iter++) {
      if (iter == 0) {
        Document doc = new Document();
        doc.add(newTextField("field1", "this is field1", Field.Store.NO));
        doc.add(newTextField("field2", "this is field2", Field.Store.NO));
        doc.add(newTextField("field3", "aaa", Field.Store.NO));
        doc.add(newTextField("field4", "bbb", Field.Store.NO));
        for(int i=0;i<DOC_COUNT;i++) {
          w.addDocument(doc);
        }
      } else {
        w.forceMerge(1);
      }

      IndexReader r = w.getReader();
      
      TermsEnum terms = MultiTerms.getTerms(r, "field3").iterator();
      assertEquals(TermsEnum.SeekStatus.END, terms.seekCeil(new BytesRef("abc")));
      r.close();
    }

    w.close();
    d.close();
  }

  public void testTermOrd() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random()))
                                         .setCodec(TestUtil.alwaysPostingsFormat(TestUtil.getDefaultPostingsFormat())));
    Document doc = new Document();
    doc.add(newTextField("f", "a b c", Field.Store.NO));
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    TermsEnum terms = getOnlyLeafReader(r).terms("f").iterator();
    assertTrue(terms.next() != null);
    try {
      assertEquals(0, terms.ord());
    } catch (UnsupportedOperationException uoe) {
      // ok -- codec is not required to support this op
    }
    r.close();
    w.close();
    d.close();
  }
}

