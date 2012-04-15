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

import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.document.*;
import org.apache.lucene.util.*;

public class TestFlex extends LuceneTestCase {

  // Test non-flex API emulated on flex index
  public void testNonFlex() throws Exception {
    Directory d = newDirectory();

    final int DOC_COUNT = 177;

    IndexWriter w = new IndexWriter(
        d,
        new IndexWriterConfig(Version.LUCENE_31, new MockAnalyzer(random())).
            setMaxBufferedDocs(7)
    );

    for(int iter=0;iter<2;iter++) {
      if (iter == 0) {
        Document doc = new Document();
        doc.add(newField("field1", "this is field1", TextField.TYPE_UNSTORED));
        doc.add(newField("field2", "this is field2", TextField.TYPE_UNSTORED));
        doc.add(newField("field3", "aaa", TextField.TYPE_UNSTORED));
        doc.add(newField("field4", "bbb", TextField.TYPE_UNSTORED));
        for(int i=0;i<DOC_COUNT;i++) {
          w.addDocument(doc);
        }
      } else {
        w.forceMerge(1);
      }

      IndexReader r = w.getReader();
      
      TermsEnum terms = MultiFields.getTerms(r, "field3").iterator(null);
      assertEquals(TermsEnum.SeekStatus.END, terms.seekCeil(new BytesRef("abc")));
      r.close();
    }

    w.close();
    d.close();
  }

  public void testTermOrd() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(TEST_VERSION_CURRENT,
                                                             new MockAnalyzer(random())).setCodec(_TestUtil.alwaysPostingsFormat(new Lucene40PostingsFormat())));
    Document doc = new Document();
    doc.add(newField("f", "a b c", TextField.TYPE_UNSTORED));
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    TermsEnum terms = getOnlySegmentReader(r).fields().terms("f").iterator(null);
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

