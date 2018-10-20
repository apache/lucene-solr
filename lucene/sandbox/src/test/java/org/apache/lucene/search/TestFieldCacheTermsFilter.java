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
package org.apache.lucene.search;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;

import java.util.ArrayList;
import java.util.List;

/**
 * A basic unit test for FieldCacheTermsFilter
 *
 * @see org.apache.lucene.search.DocValuesTermsQuery
 */
public class TestFieldCacheTermsFilter extends LuceneTestCase {
  public void testMissingTerms() throws Exception {
    String fieldName = "field1";
    Directory rd = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), rd);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int term = i * 10; //terms are units of 10;
      doc.add(newStringField(fieldName, "" + term, Field.Store.YES));
      doc.add(new SortedDocValuesField(fieldName, new BytesRef("" + term)));
      w.addDocument(doc);
    }
    IndexReader reader = w.getReader();
    w.close();

    IndexSearcher searcher = newSearcher(reader);
    int numDocs = reader.numDocs();
    ScoreDoc[] results;

    List<String> terms = new ArrayList<>();
    terms.add("5");
    results = searcher.search(new DocValuesTermsQuery(fieldName,  terms.toArray(new String[0])), numDocs).scoreDocs;
    assertEquals("Must match nothing", 0, results.length);

    terms = new ArrayList<>();
    terms.add("10");
    results = searcher.search(new DocValuesTermsQuery(fieldName,  terms.toArray(new String[0])), numDocs).scoreDocs;
    assertEquals("Must match 1", 1, results.length);

    terms = new ArrayList<>();
    terms.add("10");
    terms.add("20");
    results = searcher.search(new DocValuesTermsQuery(fieldName,  terms.toArray(new String[0])), numDocs).scoreDocs;
    assertEquals("Must match 2", 2, results.length);

    reader.close();
    rd.close();
  }
}
