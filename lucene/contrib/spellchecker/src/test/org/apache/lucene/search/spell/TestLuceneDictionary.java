package org.apache.lucene.search.spell;

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
import java.util.Iterator;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test case for LuceneDictionary.
 * It first creates a simple index and then a couple of instances of LuceneDictionary
 * on different fields and checks if all the right text comes back.
 */
public class TestLuceneDictionary extends LuceneTestCase {

  private Directory store;

  private IndexReader indexReader = null;
  private LuceneDictionary ld;
  private Iterator<String> it;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    store = newDirectory();
    IndexWriter writer = new IndexWriter(store, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));

    Document doc;

    doc = new  Document();
    doc.add(newField("aaa", "foo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(newField("aaa", "foo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(new  Field("contents", "Tom", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(new  Field("contents", "Jerry", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newField("zzz", "bar", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.optimize();
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    if (indexReader != null)
      indexReader.close();
    store.close();
    super.tearDown();
  }
  
  public void testFieldNonExistent() throws IOException {
    try {
      indexReader = IndexReader.open(store, true);

      ld = new LuceneDictionary(indexReader, "nonexistent_field");
      it = ld.getWordsIterator();

      assertFalse("More elements than expected", it.hasNext());
      assertTrue("Nonexistent element is really null", it.next() == null);
    } finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }

  public void testFieldAaa() throws IOException {
    try {
      indexReader = IndexReader.open(store, true);

      ld = new LuceneDictionary(indexReader, "aaa");
      it = ld.getWordsIterator();

      assertTrue("First element doesn't exist.", it.hasNext());
      assertTrue("First element isn't correct", it.next().equals("foo"));
      assertFalse("More elements than expected", it.hasNext());
      assertTrue("Nonexistent element is really null", it.next() == null);
    } finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }

  public void testFieldContents_1() throws IOException {
    try {
      indexReader = IndexReader.open(store, true);

      ld = new LuceneDictionary(indexReader, "contents");
      it = ld.getWordsIterator();

      assertTrue("First element doesn't exist.", it.hasNext());
      assertTrue("First element isn't correct", it.next().equals("Jerry"));
      assertTrue("Second element doesn't exist.", it.hasNext());
      assertTrue("Second element isn't correct", it.next().equals("Tom"));
      assertFalse("More elements than expected", it.hasNext());
      assertTrue("Nonexistent element is really null", it.next() == null);

      ld = new LuceneDictionary(indexReader, "contents");
      it = ld.getWordsIterator();

      int counter = 2;
      while (it.hasNext()) {
        it.next();
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
      indexReader = IndexReader.open(store, true);

      ld = new LuceneDictionary(indexReader, "contents");
      it = ld.getWordsIterator();

      // hasNext() should have no side effects
      assertTrue("First element isn't were it should be.", it.hasNext());
      assertTrue("First element isn't were it should be.", it.hasNext());
      assertTrue("First element isn't were it should be.", it.hasNext());

      // just iterate through words
      assertTrue("First element isn't correct", it.next().equals("Jerry"));
      assertTrue("Second element isn't correct", it.next().equals("Tom"));
      assertTrue("Nonexistent element is really null", it.next() == null);

      // hasNext() should still have no side effects ...
      assertFalse("There should be any more elements", it.hasNext());
      assertFalse("There should be any more elements", it.hasNext());
      assertFalse("There should be any more elements", it.hasNext());

      // .. and there are really no more words
      assertTrue("Nonexistent element is really null", it.next() == null);
      assertTrue("Nonexistent element is really null", it.next() == null);
      assertTrue("Nonexistent element is really null", it.next() == null);
    }
    finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }

  public void testFieldZzz() throws IOException {
    try {
      indexReader = IndexReader.open(store, true);

      ld = new LuceneDictionary(indexReader, "zzz");
      it = ld.getWordsIterator();

      assertTrue("First element doesn't exist.", it.hasNext());
      assertTrue("First element isn't correct", it.next().equals("bar"));
      assertFalse("More elements than expected", it.hasNext());
      assertTrue("Nonexistent element is really null", it.next() == null);
    }
    finally {
      if  (indexReader != null) { indexReader.close(); }
    }
  }
  
  public void testSpellchecker() throws IOException {
    Directory dir = newDirectory();
    SpellChecker sc = new SpellChecker(dir);
    indexReader = IndexReader.open(store, true);
    sc.indexDictionary(new LuceneDictionary(indexReader, "contents"));
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
