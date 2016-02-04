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
package org.apache.lucene.collation;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedDocValues;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.text.Collator;
import java.util.Locale;

/**
 * trivial test of CollationDocValuesField
 */
public class TestCollationDocValuesField extends LuceneTestCase {
  
  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field field = newField("field", "", StringField.TYPE_STORED);
    CollationDocValuesField collationField = new CollationDocValuesField("collated", Collator.getInstance(Locale.ENGLISH));
    doc.add(field);
    doc.add(collationField);

    field.setStringValue("ABC");
    collationField.setStringValue("ABC");
    iw.addDocument(doc);
    
    field.setStringValue("abc");
    collationField.setStringValue("abc");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher is = newSearcher(ir);
    
    SortField sortField = new SortField("collated", SortField.Type.STRING);
    
    TopDocs td = is.search(new MatchAllDocsQuery(), 5, new Sort(sortField));
    assertEquals("abc", ir.document(td.scoreDocs[0].doc).get("field"));
    assertEquals("ABC", ir.document(td.scoreDocs[1].doc).get("field"));
    ir.close();
    dir.close();
  }
  
  public void testRanges() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field field = newField("field", "", StringField.TYPE_STORED);
    Collator collator = Collator.getInstance(Locale.getDefault()); // uses -Dtests.locale
    if (random().nextBoolean()) {
      collator.setStrength(Collator.PRIMARY);
    }
    CollationDocValuesField collationField = new CollationDocValuesField("collated", collator);
    doc.add(field);
    doc.add(collationField);
    
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      String value = TestUtil.randomSimpleString(random());
      field.setStringValue(value);
      collationField.setStringValue(value);
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
        BytesRef lowerVal = new BytesRef(collator.getCollationKey(start).toByteArray());
        BytesRef upperVal = new BytesRef(collator.getCollationKey(end).toByteArray());
        doTestRanges(is, start, end, lowerVal, upperVal, collator);
      }
    } finally {
      ir.close();
      dir.close();
    }
  }
  
  private void doTestRanges(IndexSearcher is, String startPoint, String endPoint, BytesRef startBR, BytesRef endBR, Collator collator) throws Exception { 
    SortedDocValues dvs = MultiDocValues.getSortedValues(is.getIndexReader(), "collated");
    for(int docID=0;docID<is.getIndexReader().maxDoc();docID++) {
      Document doc = is.doc(docID);
      String s = doc.getField("field").stringValue();
      boolean collatorAccepts = collate(collator, s, startPoint) >= 0 && collate(collator, s, endPoint) <= 0;
      BytesRef br = dvs.get(docID);
      boolean luceneAccepts = br.compareTo(startBR) >= 0 && br.compareTo(endBR) <= 0;
      assertEquals(startPoint + " <= " + s + " <= " + endPoint, collatorAccepts, luceneAccepts);
    }
  }
}
