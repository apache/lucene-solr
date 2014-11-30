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
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests the Terms.docCount statistic
 */
public class TestDocCount extends LuceneTestCase {
  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      iw.addDocument(doc(iw));
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
  
  private Document doc(RandomIndexWriter w) {
    Document doc = w.newDocument();
    int numFields = TestUtil.nextInt(random(), 1, 10);
    FieldTypes fieldTypes = w.getFieldTypes();
    for (int i = 0; i < numFields; i++) {
      String fieldName = "" + TestUtil.nextInt(random(), 'a', 'z');
      fieldTypes.setMultiValued(fieldName);
      doc.addAtom(fieldName, "" + TestUtil.nextInt(random(), 'a', 'z'));
    }
    return doc;
  }
  
  private void verifyCount(IndexReader ir) throws Exception {
    Fields fields = MultiFields.getFields(ir);
    for (String field : fields) {
      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }
      int docCount = terms.getDocCount();
      FixedBitSet visited = new FixedBitSet(ir.maxDoc());
      TermsEnum te = terms.iterator(null);
      while (te.next() != null) {
        DocsEnum de = TestUtil.docs(random(), te, null, null, DocsEnum.FLAG_NONE);
        while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          visited.set(de.docID());
        }
      }
      assertEquals(visited.cardinality(), docCount);
    }
  }
}
