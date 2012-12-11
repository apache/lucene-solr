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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/**
 * Tests the Terms.docCount statistic
 */
@SuppressCodecs("Lucene3x")
public class TestDocCount extends LuceneTestCase {
  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      iw.addDocument(doc());
    }
    IndexReader ir = iw.getReader();
    verifyCount(ir);
    ir.close();
    iw.forceMerge(1);
    ir = iw.getReader();
    verifyCount(ir);
    ir.close();
    iw.close();
    dir.close();
  }
  
  private Document doc() {
    Document doc = new Document();
    int numFields = _TestUtil.nextInt(random(), 1, 10);
    for (int i = 0; i < numFields; i++) {
      doc.add(newStringField("" + _TestUtil.nextInt(random(), 'a', 'z'), "" + _TestUtil.nextInt(random(), 'a', 'z'), Field.Store.NO));
    }
    return doc;
  }
  
  private void verifyCount(IndexReader ir) throws Exception {
    Fields fields = MultiFields.getFields(ir);
    if (fields == null) {
      return;
    }
    for (String field : fields) {
      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }
      int docCount = terms.getDocCount();
      FixedBitSet visited = new FixedBitSet(ir.maxDoc());
      TermsEnum te = terms.iterator(null);
      while (te.next() != null) {
        DocsEnum de = _TestUtil.docs(random(), te, null, null, DocsEnum.FLAG_NONE);
        while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          visited.set(de.docID());
        }
      }
      assertEquals(visited.cardinality(), docCount);
    }
  }
}
