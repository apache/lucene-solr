package org.apache.lucene.search;

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
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/** Simple tests for SortedSetSortField, indexing the sortedset up front */
public class TestSortedSetSortField extends LuceneTestCase {
  
  public void testEmptyIndex() throws Exception {
    IndexSearcher empty = newSearcher(new MultiReader());
    Query query = new TermQuery(new Term("contents", "foo"));
  
    Sort sort = new Sort();
    sort.setSort(new SortedSetSortField("sortedset", false));
    TopDocs td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);
    
    // for an empty index, any selector should work
    for (SortedSetSelector.Type v : SortedSetSelector.Type.values()) {
      sort.setSort(new SortedSetSortField("sortedset", false, v));
      td = empty.search(query, null, 10, sort, true, true);
      assertEquals(0, td.totalHits);
    }
  }
  
  public void testEquals() throws Exception {
    SortField sf = new SortedSetSortField("a", false);
    assertFalse(sf.equals(null));
    
    assertEquals(sf, sf);
    
    SortField sf2 = new SortedSetSortField("a", false);
    assertEquals(sf, sf2);
    assertEquals(sf.hashCode(), sf2.hashCode());
    
    assertFalse(sf.equals(new SortedSetSortField("a", true)));
    assertFalse(sf.equals(new SortedSetSortField("b", false)));
    assertFalse(sf.equals(new SortedSetSortField("a", false, SortedSetSelector.Type.MAX)));
    assertFalse(sf.equals("foo"));
  }
  
  public void testForward() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = writer.newDocument();
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");
    doc.addAtom("value", new BytesRef("baz"));
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("foo"));
    doc.addAtom("value", new BytesRef("bar"));
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("value"));
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");
    Document doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("foo"));
    doc.addAtom("value", new BytesRef("bar"));
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("baz"));
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("value", true));
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'baz'
    assertEquals("2", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }
  
  public void testMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");
    fieldTypes.setSortMissingFirst("value");
    Document doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("baz"));
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("foo"));
    doc.addAtom("value", new BytesRef("bar"));
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addUniqueAtom("id", "3");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("value"));
    assertEquals(3, td.totalHits);
    // 'bar' comes before 'baz'
    // null comes first
    assertEquals("3", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }
  
  public void testMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");
    Document doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("baz"));
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("foo"));
    doc.addAtom("value", new BytesRef("bar"));
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addUniqueAtom("id", "3");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("value"));
    assertEquals(3, td.totalHits);
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("3", searcher.doc(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }
  
  public void testSingleton() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");

    Document doc = writer.newDocument();

    doc.addAtom("value", new BytesRef("baz"));
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", new BytesRef("bar"));
    doc.addUniqueAtom("id", "1");
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

    ir.close();
    dir.close();
  }
}
