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


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/** Tests for SortedSetSortField selectors other than MIN,
 *  these require optional codec support (random access to ordinals) */
@SuppressCodecs({"SimpleText"})
public class TestSortedSetSelector extends LuceneTestCase {
  
  public void testMax() throws Exception {
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

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MAX));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'baz' comes before 'foo'
    assertEquals("2", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMaxReverse() throws Exception {
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
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    
    Sort sort = new Sort(new SortedSetSortField("value", true, SortedSetSelector.Type.MAX));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'baz' comes before 'foo'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMaxMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("baz")));
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    
    SortField sortField = new SortedSetSortField("value", false, SortedSetSelector.Type.MAX);
    sortField.setMissingValue(SortField.STRING_FIRST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null comes first
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    // 'baz' comes before 'foo'
    assertEquals("3", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[2].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMaxMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("baz")));
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    
    SortField sortField = new SortedSetSortField("value", false, SortedSetSelector.Type.MAX);
    sortField.setMissingValue(SortField.STRING_LAST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // 'baz' comes before 'foo'
    assertEquals("3", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("1", searcher.doc(td.scoreDocs[2].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMaxSingleton() throws Exception {
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
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MAX));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }
  
  public void testMiddleMin() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MIN));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'b' comes before 'c'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMinReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", true, SortedSetSelector.Type.MIDDLE_MIN));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'b' comes before 'c'
    assertEquals("2", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMinMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField = new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MIN);
    sortField.setMissingValue(SortField.STRING_FIRST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null comes first
    assertEquals("3", searcher.doc(td.scoreDocs[0].doc).get("id"));
    // 'b' comes before 'c'
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[2].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMinMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField = new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MIN);
    sortField.setMissingValue(SortField.STRING_LAST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // 'b' comes before 'c'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("3", searcher.doc(td.scoreDocs[2].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMinSingleton() throws Exception {
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
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MIN));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }
  
  public void testMiddleMax() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MAX));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'b' comes before 'c'
    assertEquals("2", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMaxReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", true, SortedSetSelector.Type.MIDDLE_MAX));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'b' comes before 'c'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMaxMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField = new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MAX);
    sortField.setMissingValue(SortField.STRING_FIRST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // null comes first
    assertEquals("3", searcher.doc(td.scoreDocs[0].doc).get("id"));
    // 'b' comes before 'c'
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[2].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMaxMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", new BytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField = new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MAX);
    sortField.setMissingValue(SortField.STRING_LAST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value);
    // 'b' comes before 'c'
    assertEquals("2", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("3", searcher.doc(td.scoreDocs[2].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testMiddleMaxSingleton() throws Exception {
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
    
    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MAX));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }
}
