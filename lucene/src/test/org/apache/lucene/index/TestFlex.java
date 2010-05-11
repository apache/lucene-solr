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

import java.io.*;
import java.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.*;

public class TestFlex extends LuceneTestCase {

  // Test non-flex API emulated on flex index
  public void testNonFlex() throws Exception {
    Directory d = new MockRAMDirectory();

    final int DOC_COUNT = 177;

    IndexWriter w = new IndexWriter(d, new MockAnalyzer(),
                                    IndexWriter.MaxFieldLength.UNLIMITED);

    for(int iter=0;iter<2;iter++) {
      if (iter == 0) {
        w.setMaxBufferedDocs(7);
        Document doc = new Document();
        doc.add(new Field("field1", "this is field1", Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field("field2", "this is field2", Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field("field3", "aaa", Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field("field4", "bbb", Field.Store.NO, Field.Index.ANALYZED));
        for(int i=0;i<DOC_COUNT;i++) {
          w.addDocument(doc);
        }
      } else {
        w.optimize();
      }

      IndexReader r = w.getReader();
      TermEnum terms = r.terms(new Term("field3", "bbb"));
      // pre-flex API should seek to the next field
      assertNotNull(terms.term());
      assertEquals("field4", terms.term().field());
      
      terms = r.terms(new Term("field5", "abc"));
      assertNull(terms.term());
      r.close();
    }

    w.close();
    d.close();
  }

  public void testTermOrd() throws Exception {
    Directory d = new MockRAMDirectory();
    IndexWriter w = new IndexWriter(d, new MockAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
    Document doc = new Document();
    doc.add(new Field("f", "a b c", Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    TermsEnum terms = r.getSequentialSubReaders()[0].fields().terms("f").iterator();
    assertTrue(terms.next() != null);
    assertEquals(0, terms.ord());
    r.close();
    w.close();
    d.close();
  }
}

