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

import org.apache.lucene.document.Document2;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Simple tests for SortedNumericSortField */
public class TestSortedNumericSortField extends LuceneTestCase {
  
  public void testEmptyIndex() throws Exception {
    IndexSearcher empty = newSearcher(new MultiReader());
    Query query = new TermQuery(new Term("contents", "foo"));
  
    Sort sort = new Sort();
    sort.setSort(new SortedNumericSortField("sortednumeric", SortField.Type.LONG));
    TopDocs td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);
    
    // for an empty index, any selector should work
    for (SortedNumericSelector.Type v : SortedNumericSelector.Type.values()) {
      sort.setSort(new SortedNumericSortField("sortednumeric", SortField.Type.LONG, false, v));
      td = empty.search(query, null, 10, sort, true, true);
      assertEquals(0, td.totalHits);
    }
  }
  
  public void testEquals() throws Exception {
    SortField sf = new SortedNumericSortField("a", SortField.Type.LONG);
    assertFalse(sf.equals(null));
    
    assertEquals(sf, sf);
    
    SortField sf2 = new SortedNumericSortField("a", SortField.Type.LONG);
    assertEquals(sf, sf2);
    assertEquals(sf.hashCode(), sf2.hashCode());
    
    assertFalse(sf.equals(new SortedNumericSortField("a", SortField.Type.LONG, true)));
    assertFalse(sf.equals(new SortedNumericSortField("a", SortField.Type.FLOAT)));
    assertFalse(sf.equals(new SortedNumericSortField("b", SortField.Type.LONG)));
    assertFalse(sf.equals(new SortedNumericSortField("a", SortField.Type.LONG, false, SortedNumericSelector.Type.MAX)));
    assertFalse(sf.equals("foo"));
  }
  
  public void testForward() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);    
    FieldTypes fieldTypes = writer.getFieldTypes();     
    fieldTypes.setMultiValued("value");
    Document2 doc = writer.newDocument();
    doc.addInt("value", 5);
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addInt("value", 3);
    doc.addInt("value", 7);
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedNumericSortField("value", SortField.Type.INT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 3 comes before 5
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
    Document2 doc = writer.newDocument();
    doc.addInt("value", 3);
    doc.addInt("value", 7);
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addInt("value", 5);
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedNumericSortField("value", SortField.Type.INT, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
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
    fieldTypes.enableSorting("value");
    fieldTypes.setSortMissingFirst("value");

    Document2 doc = writer.newDocument();
    doc.addInt("value", 5);
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addInt("value", 3);
    doc.addInt("value", 7);
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addUniqueAtom("id", "3");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // 3 comes before 5
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
    Document2 doc = writer.newDocument();
    doc.addInt("value", 5);
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addInt("value", 3);
    doc.addInt("value", 7);
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addUniqueAtom("id", "3");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // 3 comes before 5
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
    Document2 doc = writer.newDocument();
    doc.addInt("value", 5);
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addInt("value", 3);
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 3 comes before 5
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }
  
  public void testFloat() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");

    Document2 doc = writer.newDocument();
    doc.addFloat("value", -3f);
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addFloat("value", -5f);
    doc.addFloat("value", 7f);
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");
    System.out.println("sort: " + sort);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // -5 comes before -3
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
  
  public void testDouble() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");

    Document2 doc = writer.newDocument();
    doc.addDouble("value", -3d);
    doc.addUniqueAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addDouble("value", -5d);
    doc.addDouble("value", 7d);
    doc.addUniqueAtom("id", "1");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // -5 comes before -3
    assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.doc(td.scoreDocs[1].doc).get("id"));
    
    ir.close();
    dir.close();
  }
}
