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

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * Test case for LuceneDictionary.
 * It first creates a simple index and then a couple of instances of LuceneDictionary
 * on different fields and checks if all the right text comes back.
 *
 */
public class TestLuceneDictionary extends TestCase {

  private Directory store = new RAMDirectory();

  private IndexReader indexReader = null;

  private LuceneDictionary ld;
  private Iterator it;

  public void setUp() throws Exception {

    IndexWriter writer = new IndexWriter(store, new WhitespaceAnalyzer(), true);

    Document doc;

    doc = new  Document();
    doc.add(new Field("aaa", "foo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(new Field("aaa", "foo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(new  Field("contents", "Tom", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new  Document();
    doc.add(new  Field("contents", "Jerry", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("zzz", "bar", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.optimize();
    writer.close();
  }

  public void testFieldNonExistent() throws IOException {
    try {
      indexReader = IndexReader.open(store);

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
      indexReader = IndexReader.open(store);

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
      indexReader = IndexReader.open(store);

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
      indexReader = IndexReader.open(store);

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
      indexReader = IndexReader.open(store);

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
    SpellChecker sc = new SpellChecker(new RAMDirectory());
    indexReader = IndexReader.open(store);
    sc.indexDictionary(new LuceneDictionary(indexReader, "contents"));
    String[] suggestions = sc.suggestSimilar("Tam", 1);
    assertEquals(1, suggestions.length);
    assertEquals("Tom", suggestions[0]);
    suggestions = sc.suggestSimilar("Jarry", 1);
    assertEquals(1, suggestions.length);
    assertEquals("Jerry", suggestions[0]);
    indexReader.close();
  }
  
}
