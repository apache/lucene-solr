package org.apache.lucene.document;

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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestDateFields extends LuceneTestCase {
  static SimpleDateFormat parser = new SimpleDateFormat("MM/dd/yyyy", Locale.ROOT);
  static {
    parser.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  public void testDateSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    Date date0 = parser.parse("10/22/2014");
    doc.addDate("date", date0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    Date date1 = parser.parse("10/21/2015");
    doc.addDate("date", date1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    w.getFieldTypes().enableSorting("date", true);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("date"));
    assertEquals(2, hits.totalHits);
    Document hit = s.doc(hits.scoreDocs[0].doc);
    assertEquals("1", hit.getString("id"));
    assertEquals(date1, hit.getDate("date"));
    hit = s.doc(hits.scoreDocs[1].doc);
    assertEquals("0", hit.getString("id"));
    assertEquals(date0, hit.getDate("date"));
    r.close();
    w.close();
    dir.close();
  }

  public void testDateRangeFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    Date date0 = parser.parse("10/22/2014");
    doc.addDate("date", date0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    Date date1 = parser.parse("10/21/2015");
    doc.addDate("date", date1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("date", date0, true, date1, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("date", date0, true, date1, false), 1).totalHits);
    assertEquals(0, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("date", date0, false, date1, false), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("date", parser.parse("10/21/2014"), false, parser.parse("10/23/2014"), false), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testDateDocValuesRangeFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    Date date0 = parser.parse("10/22/2014");
    doc.addDate("date", date0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    Date date1 = parser.parse("10/21/2015");
    doc.addDate("date", date1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newDocValuesRangeFilter("date", date0, true, date1, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newDocValuesRangeFilter("date", date0, true, date1, false), 1).totalHits);
    assertEquals(0, s.search(new MatchAllDocsQuery(), fieldTypes.newDocValuesRangeFilter("date", date0, false, date1, false), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newDocValuesRangeFilter("date", parser.parse("10/21/2014"), false, parser.parse("10/23/2014"), false), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedSort() throws Exception {
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("num");

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addDate("num", parser.parse("10/25/2014"));
    doc.addDate("num", parser.parse("10/2/2014"));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addDate("num", parser.parse("10/10/2014"));
    doc.addDate("num", parser.parse("10/20/2014"));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));

    // Default selector is MIN:
    assertEquals(0, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(2, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    fieldTypes.setMultiValuedNumericSortSelector("num", SortedNumericSelector.Type.MAX);
    hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));
    assertEquals(2, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(0, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    r.close();
    w.close();
  }

  public void testJustStored() throws Exception {
    Date date = parser.parse("10/22/2014");

    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredDate("num", date);
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    doc = s.doc(0);
    assertEquals(date, doc.getDate("num"));
    r.close();
    w.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addDate("num", parser.parse("10/22/2014"));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addStoredDate("num", parser.parse("10/27/2014")),
               "field \"num\": cannot addStored: field was already added non-stored");
    w.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredDate("num", parser.parse("10/22/2014"));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addDate("num", parser.parse("10/27/2014")),
               "field \"num\": this field is only stored; use addStoredXXX instead");
    w.close();
  }
}
