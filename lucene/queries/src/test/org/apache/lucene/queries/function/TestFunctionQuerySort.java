package org.apache.lucene.queries.function;

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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Test that functionquery's getSortField() actually works */
public class TestFunctionQuerySort extends LuceneTestCase {

  public void testSearchAfterWhenSortingByFunctionValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
    iwc.setMergePolicy(newLogMergePolicy()); // depends on docid order
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    Document doc = new Document();
    Field field = new StringField("value", "", Field.Store.YES);
    doc.add(field);

    // Save docs unsorted (decreasing value n, n-1, ...)
    final int NUM_VALS = 5;
    for (int val = NUM_VALS; val > 0; val--) {
      field.setStringValue(Integer.toString(val));
      writer.addDocument(doc);
    }

    // Open index
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    // Get ValueSource from FieldCache
    IntFieldSource src = new IntFieldSource("value");
    // ...and make it a sort criterion
    SortField sf = src.getSortField(false).rewrite(searcher);
    Sort orderBy = new Sort(sf);

    // Get hits sorted by our FunctionValues (ascending values)
    Query q = new MatchAllDocsQuery();
    TopDocs hits = searcher.search(q, reader.maxDoc(), orderBy);
    assertEquals(NUM_VALS, hits.scoreDocs.length);
    // Verify that sorting works in general
    int i = 0;
    for (ScoreDoc hit : hits.scoreDocs) {
      int valueFromDoc = Integer.parseInt(reader.document(hit.doc).get("value"));
      assertEquals(++i, valueFromDoc);
    }

    // Now get hits after hit #2 using IS.searchAfter()
    int afterIdx = 1;
    FieldDoc afterHit = (FieldDoc) hits.scoreDocs[afterIdx];
    hits = searcher.searchAfter(afterHit, q, reader.maxDoc(), orderBy);

    // Expected # of hits: NUM_VALS - 2
    assertEquals(NUM_VALS - (afterIdx + 1), hits.scoreDocs.length);

    // Verify that hits are actually "after"
    int afterValue = ((Double) afterHit.fields[0]).intValue();
    for (ScoreDoc hit : hits.scoreDocs) {
      int val = Integer.parseInt(reader.document(hit.doc).get("value"));
      assertTrue(afterValue <= val);
      assertFalse(hit.doc == afterHit.doc);
    }
    reader.close();
    dir.close();
  }
}
