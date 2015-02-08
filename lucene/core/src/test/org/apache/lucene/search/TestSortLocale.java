package org.apache.lucene.search;

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

import java.io.IOException;
import java.text.Collator;
import java.util.Locale;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestSortLocale extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setSortLocale("collated", Locale.ENGLISH, Collator.IDENTICAL);

    Document doc = w.newDocument();
    doc.addAtom("field", "ABC");
    doc.addAtom("collated", "ABC");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("field", "abc");
    doc.addAtom("collated", "abc");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs td = s.search(new MatchAllDocsQuery(), 5, fieldTypes.newSort("collated"));
    assertEquals("abc", r.document(td.scoreDocs[0].doc).get("field"));
    assertEquals("ABC", r.document(td.scoreDocs[1].doc).get("field"));
    r.close();
    w.close();
    dir.close();
  }

  public void testRanges() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = iw.getFieldTypes();

    Collator collator = Collator.getInstance(Locale.getDefault());
    if (random().nextBoolean()) {
      collator.setStrength(Collator.PRIMARY);
    }
    fieldTypes.setSortLocale("collated", Locale.getDefault(), collator.getStrength()); // uses -Dtests.locale
    
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = iw.newDocument();
      String value = TestUtil.randomSimpleString(random());
      doc.addAtom("field", value);
      doc.addAtom("collated", value);
      iw.addDocument(doc);
    }
    
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    int numChecks = atLeast(100);

    try {
      for (int i = 0; i < numChecks; i++) {
        String start = TestUtil.randomSimpleString(random());
        String end = TestUtil.randomSimpleString(random());
        Query query = new ConstantScoreQuery(fieldTypes.newStringDocValuesRangeFilter("collated", start, true, end, true));
        doTestRanges(is, start, end, query, collator);
      }
    } finally {    
      ir.close();
      dir.close();
    }
  }
  
  private void doTestRanges(IndexSearcher is, String startPoint, String endPoint, Query query, Collator collator) throws Exception { 
    QueryUtils.check(query);
    
    // positive test
    TopDocs docs = is.search(query, is.getIndexReader().maxDoc());
    for (ScoreDoc doc : docs.scoreDocs) {
      String value = is.doc(doc.doc).getString("field");
      assertTrue(collate(collator, value, startPoint) >= 0);
      assertTrue(collate(collator, value, endPoint) <= 0);
    }
    
    // negative test
    BooleanQuery bq = new BooleanQuery();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(query, BooleanClause.Occur.MUST_NOT);
    docs = is.search(bq, is.getIndexReader().maxDoc());
    for (ScoreDoc doc : docs.scoreDocs) {
      String value = is.doc(doc.doc).getString("field");
      assertTrue(collate(collator, value, startPoint) < 0 || collate(collator, value, endPoint) > 0);
    }
  }
}
