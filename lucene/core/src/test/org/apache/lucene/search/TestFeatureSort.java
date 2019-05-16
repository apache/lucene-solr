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

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/*
 * Very simple tests of sorting.
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

  /** Tests sorting on type feature */
  public void testFeature() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
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
    Sort sort = new Sort(new SortField("field", new FeatureComparatorSource(new BytesRef("name"), null)));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // numeric order
    assertEquals("30.1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  /** Tests sorting on type feature in reverse */
  public void testFeatureReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
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
    Sort sort = new Sort(new SortField("field", new FeatureComparatorSource(new BytesRef("name"), null), true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // numeric order
    assertEquals("1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("30.1", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  /** Tests sorting on type float, specifying the missing value should be treated as Float.MAX_VALUE */
  public void testFeatureMissing() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
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
    SortField sortField = new SortField("field", new FeatureComparatorSource(new BytesRef("name"), Float.MAX_VALUE));
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null is treated as 0
    assertNull(searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  /** Tests sorting on type float, specifying the missing value should be treated as Float.MAX_VALUE */
  public void testFeatureMissingFieldInSegment() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
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
    SortField sortField = new SortField("field", new FeatureComparatorSource(new BytesRef("name"), Float.MAX_VALUE));
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null is treated as 0
    assertNull(searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  /** Tests sorting on type float, specifying the missing value should be treated as Float.MAX_VALUE */
  public void testFeatureMissingFeatureNameInSegment() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
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
    SortField sortField = new SortField("field", new FeatureComparatorSource(new BytesRef("name"), Float.MAX_VALUE));
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null is treated as 0
    assertNull(searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }

  /** Tests sorting on type feature with a missing value */
  public void testFeatureMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
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
    Sort sort = new Sort(new SortField("field", new FeatureComparatorSource(new BytesRef("name"), null)));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null is treated as 0
    assertEquals("4.2", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("1.3", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
}
