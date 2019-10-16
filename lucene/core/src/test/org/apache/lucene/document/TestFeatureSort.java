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
package org.apache.lucene.document;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/*
 * Test for sorting using a feature from a FeatureField.
 *
 * THE RULES:
 * 1. keywords like 'abstract' and 'static' should not appear in this file.
 * 2. each test method should be self-contained and understandable.
 * 3. no test methods should share code with other test methods.
 * 4. no testing of things unrelated to sorting.
 * 5. no tracers.
 * 6. keyword 'class' should appear only once in this file, here ----
 *                                                                  |
 *        -----------------------------------------------------------
 *        |
 *       \./
 */
public class TestFeatureSort extends LuceneTestCase {

  public void testFeature() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    doc.add(new FeatureField("field", "name", 30.1F));
    doc.add(newStringField("value", "30.1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1.3F));
    doc.add(newStringField("value", "1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4.2F));
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(FeatureField.newFeatureSort("field", "name"));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // numeric order
    assertEquals("30.1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  public void testFeatureMissing() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1.3F));
    doc.add(newStringField("value", "1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4.2F));
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(FeatureField.newFeatureSort("field", "name"));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null is treated as 0
    assertEquals("4.2", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  public void testFeatureMissingFieldInSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    writer.addDocument(doc);
    writer.commit();
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1.3F));
    doc.add(newStringField("value", "1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4.2F));
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(FeatureField.newFeatureSort("field", "name"));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null is treated as 0
    assertEquals("4.2", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  public void testFeatureMissingFeatureNameInSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    doc.add(new FeatureField("field", "different_name", 0.5F));
    writer.addDocument(doc);
    writer.commit();
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1.3F));
    doc.add(newStringField("value", "1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4.2F));
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(FeatureField.newFeatureSort("field", "name"));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null is treated as 0
    assertEquals("4.2", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  public void testFeatureMultipleMissing() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1.3F));
    doc.add(newStringField("value", "1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4.2F));
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(FeatureField.newFeatureSort("field", "name"));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(7, td.totalHits.value);
    // null is treated as 0
    assertEquals("4.2", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[3].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[4].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[5].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[6].doc).get("value"));

    ir.close();
    dir.close();
  }

  // This duel gives compareBottom and compareTop some coverage
  public void testDuelFloat() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(100);
    for (int d = 0; d < numDocs; ++d) {
      Document doc = new Document();
      if (random().nextBoolean()) {
        float f;
        do {
          int freq = TestUtil.nextInt(random(), 1, FeatureField.MAX_FREQ);
          f = FeatureField.decodeFeatureValue(freq);
        } while (f < Float.MIN_NORMAL);
        doc.add(new NumericDocValuesField("float", Float.floatToIntBits(f)));
        doc.add(new FeatureField("feature", "foo", f));
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(r);

    TopDocs topDocs = null;
    TopDocs featureTopDocs = null;
    do {
      if (topDocs == null) {
        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
            new Sort(new SortField("float", SortField.Type.FLOAT, true)));
        featureTopDocs = searcher.search(new MatchAllDocsQuery(), 10,
            new Sort(FeatureField.newFeatureSort("feature", "foo")));
      } else {
        topDocs = searcher.searchAfter(topDocs.scoreDocs[topDocs.scoreDocs.length - 1],
            new MatchAllDocsQuery(), 10,
            new Sort(new SortField("float", SortField.Type.FLOAT, true)));
        featureTopDocs = searcher.searchAfter(featureTopDocs.scoreDocs[featureTopDocs.scoreDocs.length - 1],
            new MatchAllDocsQuery(), 10,
            new Sort(FeatureField.newFeatureSort("feature", "foo")));
      }

      CheckHits.checkEqual(new MatchAllDocsQuery(), topDocs.scoreDocs, featureTopDocs.scoreDocs);
    } while (topDocs.scoreDocs.length > 0);

    r.close();
    dir.close();
  }
}
