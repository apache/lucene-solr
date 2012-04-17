package org.apache.lucene.sandbox.queries;

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

import java.io.IOException;
import java.util.HashSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class DuplicateFilterTest extends LuceneTestCase {
  private static final String KEY_FIELD = "url";
  private Directory directory;
  private IndexReader reader;
  TermQuery tq = new TermQuery(new Term("text", "lucene"));
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    //Add series of docs with filterable fields : url, text and dates  flags
    addDoc(writer, "http://lucene.apache.org", "lucene 1.4.3 available", "20040101");
    addDoc(writer, "http://lucene.apache.org", "New release pending", "20040102");
    addDoc(writer, "http://lucene.apache.org", "Lucene 1.9 out now", "20050101");
    addDoc(writer, "http://www.bar.com", "Local man bites dog", "20040101");
    addDoc(writer, "http://www.bar.com", "Dog bites local man", "20040102");
    addDoc(writer, "http://www.bar.com", "Dog uses Lucene", "20050101");
    addDoc(writer, "http://lucene.apache.org", "Lucene 2.0 out", "20050101");
    addDoc(writer, "http://lucene.apache.org", "Oops. Lucene 2.1 out", "20050102");

    // Until we fix LUCENE-2348, the index must
    // have only 1 segment:
    writer.forceMerge(1);

    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);

  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  private void addDoc(RandomIndexWriter writer, String url, String text, String date) throws IOException {
    Document doc = new Document();
    doc.add(newField(KEY_FIELD, url, StringField.TYPE_STORED));
    doc.add(newField("text", text, TextField.TYPE_STORED));
    doc.add(newField("date", date, TextField.TYPE_STORED));
    writer.addDocument(doc);
  }

  public void testDefaultFilter() throws Throwable {
    DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
    HashSet<String> results = new HashSet<String>();
    ScoreDoc[] hits = searcher.search(tq, df, 1000).scoreDocs;

    for (ScoreDoc hit : hits) {
      Document d = searcher.doc(hit.doc);
      String url = d.get(KEY_FIELD);
      assertFalse("No duplicate urls should be returned", results.contains(url));
      results.add(url);
    }
  }

  public void testNoFilter() throws Throwable {
    HashSet<String> results = new HashSet<String>();
    ScoreDoc[] hits = searcher.search(tq, null, 1000).scoreDocs;
    assertTrue("Default searching should have found some matches", hits.length > 0);
    boolean dupsFound = false;

    for (ScoreDoc hit : hits) {
      Document d = searcher.doc(hit.doc);
      String url = d.get(KEY_FIELD);
      if (!dupsFound)
        dupsFound = results.contains(url);
      results.add(url);
    }
    assertTrue("Default searching should have found duplicate urls", dupsFound);
  }

  public void testFastFilter() throws Throwable {
    DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
    df.setProcessingMode(DuplicateFilter.ProcessingMode.PM_FAST_INVALIDATION);
    HashSet<String> results = new HashSet<String>();
    ScoreDoc[] hits = searcher.search(tq, df, 1000).scoreDocs;
    assertTrue("Filtered searching should have found some matches", hits.length > 0);

    for (ScoreDoc hit : hits) {
      Document d = searcher.doc(hit.doc);
      String url = d.get(KEY_FIELD);
      assertFalse("No duplicate urls should be returned", results.contains(url));
      results.add(url);
    }
    assertEquals("Two urls found", 2, results.size());
  }

  public void testKeepsLastFilter() throws Throwable {
    DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
    df.setKeepMode(DuplicateFilter.KeepMode.KM_USE_LAST_OCCURRENCE);
    ScoreDoc[] hits = searcher.search(tq, df, 1000).scoreDocs;
    assertTrue("Filtered searching should have found some matches", hits.length > 0);
    for (ScoreDoc hit : hits) {
      Document d = searcher.doc(hit.doc);
      String url = d.get(KEY_FIELD);
      DocsEnum td = _TestUtil.docs(random(), reader,
                                   KEY_FIELD,
                                   new BytesRef(url),
                                   MultiFields.getLiveDocs(reader),
                                   null,
                                   false);

      int lastDoc = 0;
      while (td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        lastDoc = td.docID();
      }
      assertEquals("Duplicate urls should return last doc", lastDoc, hit.doc);
    }
  }


  public void testKeepsFirstFilter() throws Throwable {
    DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
    df.setKeepMode(DuplicateFilter.KeepMode.KM_USE_FIRST_OCCURRENCE);
    ScoreDoc[] hits = searcher.search(tq, df, 1000).scoreDocs;
    assertTrue("Filtered searching should have found some matches", hits.length > 0);
    for (ScoreDoc hit : hits) {
      Document d = searcher.doc(hit.doc);
      String url = d.get(KEY_FIELD);
      DocsEnum td = _TestUtil.docs(random(), reader,
                                   KEY_FIELD,
                                   new BytesRef(url),
                                   MultiFields.getLiveDocs(reader),
                                   null,
                                   false);

      int lastDoc = 0;
      td.nextDoc();
      lastDoc = td.docID();
      assertEquals("Duplicate urls should return first doc", lastDoc, hit.doc);
    }
  }


}
