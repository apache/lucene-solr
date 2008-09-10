package org.apache.lucene;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.HitIterator;

import java.util.NoSuchElementException;

/**
 * This test intentionally not put in the search package in order
 * to test HitIterator and Hit package protection.
 * 
 * @deprecated Hits will be removed in Lucene 3.0 
 */
public class TestHitIterator extends LuceneTestCase {
  public void testIterator() throws Exception {
    RAMDirectory directory = new RAMDirectory();

    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true,
                                         IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("field", "iterator test doc 1", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("field", "iterator test doc 2", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.close();

    _TestUtil.checkIndex(directory);

    IndexSearcher searcher = new IndexSearcher(directory);
    Hits hits = searcher.search(new TermQuery(new Term("field", "iterator")));

    HitIterator iterator = (HitIterator) hits.iterator();
    assertEquals(2, iterator.length());
    assertTrue(iterator.hasNext());
    Hit hit = (Hit) iterator.next();
    assertEquals("iterator test doc 1", hit.get("field"));

    assertTrue(iterator.hasNext());
    hit = (Hit) iterator.next();
    assertEquals("iterator test doc 2", hit.getDocument().get("field"));

    assertFalse(iterator.hasNext());

    boolean caughtException = false;
    try {
      iterator.next();
    } catch (NoSuchElementException e) {
      assertTrue(true);
      caughtException = true;
    }

    assertTrue(caughtException);
  }
}
