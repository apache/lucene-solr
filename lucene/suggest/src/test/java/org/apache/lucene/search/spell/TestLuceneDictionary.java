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
package org.apache.lucene.search.spell;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test case for LuceneDictionary.
 * It first creates a simple index and then a couple of instances of LuceneDictionary
 * on different fields and checks if all the right text comes back.
 */
public class TestLuceneDictionary extends LuceneTestCase {

  private Directory store;
  private Analyzer analyzer;
  private IndexReader indexReader = null;
  private LuceneDictionary ld;
  private BytesRefIterator it;
  private BytesRef spare = new BytesRef();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    store = newDirectory();
    analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    IndexWriter writer = new IndexWriter(store, newIndexWriterConfig(analyzer));

    Document doc;

    doc = new  Document();
    doc.add(newTextField("aaa", "foo", Field.Store.YES));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(newTextField("aaa", "foo", Field.Store.YES));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(newTextField("contents", "Tom", Field.Store.YES));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(newTextField("contents", "Jerry", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("zzz", "bar", Field.Store.YES));
    writer.addDocument(doc);

    writer.forceMerge(1);
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    if (indexReader != null)
      indexReader.close();
    store.close();
    analyzer.close();
    super.tearDown();
  }
  
  public void testFieldNonExistent() throws IOException {
    try {
      indexReader = DirectoryReader.open(store);

      ld = new LuceneDictionary(indexReader, "nonexistent_field");
      it = ld.getEntryIterator();

      assertNull("More elements than expected", spare = it.next());
    } finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }

  public void testFieldAaa() throws IOException {
    try {
      indexReader = DirectoryReader.open(store);

      ld = new LuceneDictionary(indexReader, "aaa");
      it = ld.getEntryIterator();
      assertNotNull("First element doesn't exist.", spare = it.next());
      assertTrue("First element isn't correct", spare.utf8ToString().equals("foo"));
      assertNull("More elements than expected", it.next());
    } finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }

  public void testFieldContents_1() throws IOException {
    try {
      indexReader = DirectoryReader.open(store);

      ld = new LuceneDictionary(indexReader, "contents");
      it = ld.getEntryIterator();

      assertNotNull("First element doesn't exist.", spare = it.next());
      assertTrue("First element isn't correct", spare.utf8ToString().equals("Jerry"));
      assertNotNull("Second element doesn't exist.", spare = it.next());
      assertTrue("Second element isn't correct", spare.utf8ToString().equals("Tom"));
      assertNull("More elements than expected", it.next());

      ld = new LuceneDictionary(indexReader, "contents");
      it = ld.getEntryIterator();

      int counter = 2;
      while (it.next() != null) {
        counter--;
      }

      assertTrue("Number of words incorrect", counter == 0);
    }
    finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }

  public void testFieldContents_2() throws IOException {
    try {
      indexReader = DirectoryReader.open(store);

      ld = new LuceneDictionary(indexReader, "contents");
      it = ld.getEntryIterator();

      // just iterate through words
      assertEquals("First element isn't correct", "Jerry", it.next().utf8ToString());
      assertEquals("Second element isn't correct",  "Tom", it.next().utf8ToString());
      assertNull("Nonexistent element is really null", it.next());
    }
    finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }

  public void testFieldZzz() throws IOException {
    try {
      indexReader = DirectoryReader.open(store);

      ld = new LuceneDictionary(indexReader, "zzz");
      it = ld.getEntryIterator();

      assertNotNull("First element doesn't exist.", spare = it.next());
      assertEquals("First element isn't correct", "bar", spare.utf8ToString());
      assertNull("More elements than expected", it.next());
    }
    finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }
  
  public void testSpellchecker() throws IOException {
    Directory dir = newDirectory();
    SpellChecker sc = new SpellChecker(dir);
    indexReader = DirectoryReader.open(store);
    sc.indexDictionary(new LuceneDictionary(indexReader, "contents"), newIndexWriterConfig(null), false);
    String[] suggestions = sc.suggestSimilar("Tam", 1);
    assertEquals(1, suggestions.length);
    assertEquals("Tom", suggestions[0]);
    suggestions = sc.suggestSimilar("Jarry", 1);
    assertEquals(1, suggestions.length);
    assertEquals("Jerry", suggestions[0]);
    indexReader.close();
    sc.close();
    dir.close();
  }
  
}
