package org.apache.lucene.sandbox.queries;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/** Simple tests for SortedSetSortField, indexing the sortedset up front */
@SuppressCodecs({"Lucene40", "Lucene41", "Appending", "Lucene3x"}) // avoid codecs that don't support sortedset
public class TestSortedSetSortFieldDocValues extends LuceneTestCase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // ensure there is nothing in fieldcache before test starts
    FieldCache.DEFAULT.purgeAllCaches();
  }
  
  private void assertNoFieldCaches() {
    // docvalues sorting should NOT create any fieldcache entries!
    assertEquals(0, FieldCache.DEFAULT.getCacheEntries().length);
  }
  
  public void testForward() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedSetSortField("value", false));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertNoFieldCaches();
    
    ir.close();
    dir.close();
  }
  
  public void testReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedSetSortField("value", true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'baz'
    assertEquals("2", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  public void testMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    SortField sortField = new SortedSetSortField("value", false);
    sortField.setMissingValue(SortField.STRING_FIRST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // 'bar' comes before 'baz'
    // null comes first
    assertEquals("3", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[2].doc).get("id"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  public void testMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    SortField sortField = new SortedSetSortField("value", false);
    sortField.setMissingValue(SortField.STRING_LAST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("3", searcher.doc(td.scoreDocs[2].doc).get("id"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  public void testSingleton() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedSetSortField("value", false));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
}
