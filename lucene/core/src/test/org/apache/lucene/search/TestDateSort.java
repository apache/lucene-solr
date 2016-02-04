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


import java.util.Arrays;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;

/**
 * Test date sorting, i.e. auto-sorting of fields with type "long".
 * See http://issues.apache.org/jira/browse/LUCENE-1045 
 */
public class TestDateSort extends LuceneTestCase {

  private static final String TEXT_FIELD = "text";
  private static final String DATE_TIME_FIELD = "dateTime";

  private Directory directory;
  private IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Create an index writer.
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

    // oldest doc:
    // Add the first document.  text = "Document 1"  dateTime = Oct 10 03:25:22 EDT 2007
    writer.addDocument(createDocument("Document 1", 1192001122000L));
    // Add the second document.  text = "Document 2"  dateTime = Oct 10 03:25:26 EDT 2007 
    writer.addDocument(createDocument("Document 2", 1192001126000L));
    // Add the third document.  text = "Document 3"  dateTime = Oct 11 07:12:13 EDT 2007 
    writer.addDocument(createDocument("Document 3", 1192101133000L));
    // Add the fourth document.  text = "Document 4"  dateTime = Oct 11 08:02:09 EDT 2007
    writer.addDocument(createDocument("Document 4", 1192104129000L));
    // latest doc:
    // Add the fifth document.  text = "Document 5"  dateTime = Oct 12 13:25:43 EDT 2007
    writer.addDocument(createDocument("Document 5", 1192209943000L));

    reader = writer.getReader();
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  public void testReverseDateSort() throws Exception {
    IndexSearcher searcher = newSearcher(reader);

    Sort sort = new Sort(new SortField(DATE_TIME_FIELD, SortField.Type.STRING, true));
    Query query = new TermQuery(new Term(TEXT_FIELD, "document"));

    // Execute the search and process the search results.
    String[] actualOrder = new String[5];
    ScoreDoc[] hits = searcher.search(query, 1000, sort).scoreDocs;
    for (int i = 0; i < hits.length; i++) {
      Document document = searcher.doc(hits[i].doc);
      String text = document.get(TEXT_FIELD);
      actualOrder[i] = text;
    }

    // Set up the expected order (i.e. Document 5, 4, 3, 2, 1).
    String[] expectedOrder = new String[5];
    expectedOrder[0] = "Document 5";
    expectedOrder[1] = "Document 4";
    expectedOrder[2] = "Document 3";
    expectedOrder[3] = "Document 2";
    expectedOrder[4] = "Document 1";

    assertEquals(Arrays.asList(expectedOrder), Arrays.asList(actualOrder));
  }

  private Document createDocument(String text, long time) {
    Document document = new Document();

    // Add the text field.
    Field textField = newTextField(TEXT_FIELD, text, Field.Store.YES);
    document.add(textField);

    // Add the date/time field.
    String dateTimeString = DateTools.timeToString(time, DateTools.Resolution.SECOND);
    Field dateTimeField = newStringField(DATE_TIME_FIELD, dateTimeString, Field.Store.YES);
    document.add(dateTimeField);
    document.add(new SortedDocValuesField(DATE_TIME_FIELD, new BytesRef(dateTimeString)));

    return document;
  }

}
