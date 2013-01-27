package org.apache.lucene.collation;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldCacheRangeFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;

/**
 * trivial test of ICUCollationDocValuesField
 */
@SuppressCodecs("Lucene3x")
public class TestICUCollationDocValuesField extends LuceneTestCase {
  
  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field field = newField("field", "", StringField.TYPE_STORED);
    ICUCollationDocValuesField collationField = new ICUCollationDocValuesField("collated", Collator.getInstance(ULocale.ENGLISH));
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
    Collator collator = Collator.getInstance(); // uses -Dtests.locale
    if (random().nextBoolean()) {
      collator.setStrength(Collator.PRIMARY);
    }
    ICUCollationDocValuesField collationField = new ICUCollationDocValuesField("collated", collator);
    doc.add(field);
    doc.add(collationField);
    
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      String value = _TestUtil.randomSimpleString(random());
      field.setStringValue(value);
      collationField.setStringValue(value);
      iw.addDocument(doc);
    }
    
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    int numChecks = atLeast(100);
    for (int i = 0; i < numChecks; i++) {
      String start = _TestUtil.randomSimpleString(random());
      String end = _TestUtil.randomSimpleString(random());
      BytesRef lowerVal = new BytesRef(collator.getCollationKey(start).toByteArray());
      BytesRef upperVal = new BytesRef(collator.getCollationKey(end).toByteArray());
      Query query = new ConstantScoreQuery(FieldCacheRangeFilter.newBytesRefRange("collated", lowerVal, upperVal, true, true));
      doTestRanges(is, start, end, query, collator);
    }
    
    ir.close();
    dir.close();
  }
  
  private void doTestRanges(IndexSearcher is, String startPoint, String endPoint, Query query, Collator collator) throws Exception { 
    QueryUtils.check(query);
    
    // positive test
    TopDocs docs = is.search(query, is.getIndexReader().maxDoc());
    for (ScoreDoc doc : docs.scoreDocs) {
      String value = is.doc(doc.doc).get("field");
      assertTrue(collator.compare(value, startPoint) >= 0);
      assertTrue(collator.compare(value, endPoint) <= 0);
    }
    
    // negative test
    BooleanQuery bq = new BooleanQuery();
    bq.add(new MatchAllDocsQuery(), Occur.SHOULD);
    bq.add(query, Occur.MUST_NOT);
    docs = is.search(bq, is.getIndexReader().maxDoc());
    for (ScoreDoc doc : docs.scoreDocs) {
      String value = is.doc(doc.doc).get("field");
      assertTrue(collator.compare(value, startPoint) < 0 || collator.compare(value, endPoint) > 0);
    }
  }
}
