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
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestSortRescorer extends LuceneTestCase {
  IndexSearcher searcher;
  DirectoryReader reader;
  Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    doc.add(newTextField("body", "some contents and more contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 5));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(newStringField("id", "2", Field.Store.YES));
    doc.add(newTextField("body", "another document with different contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 20));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    doc.add(newTextField("body", "crappy contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 2));
    iw.addDocument(doc);
    
    reader = iw.getReader();
    searcher = new IndexSearcher(reader);
    iw.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  public void testBasic() throws Exception {

    // create a sort field and sort by it (reverse order)
    Query query = new TermQuery(new Term("body", "contents"));
    IndexReader r = searcher.getIndexReader();

    // Just first pass query
    TopDocs hits = searcher.search(query, 10);
    assertEquals(3, hits.totalHits);
    assertEquals("3", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", r.document(hits.scoreDocs[1].doc).get("id"));
    assertEquals("2", r.document(hits.scoreDocs[2].doc).get("id"));

    // Now, rescore:
    Sort sort = new Sort(new SortField("popularity", SortField.Type.INT, true));
    Rescorer rescorer = new SortRescorer(sort);
    hits = rescorer.rescore(searcher, hits, 10);
    assertEquals(3, hits.totalHits);
    assertEquals("2", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", r.document(hits.scoreDocs[1].doc).get("id"));
    assertEquals("3", r.document(hits.scoreDocs[2].doc).get("id"));

    String expl = rescorer.explain(searcher,
                                   searcher.explain(query, hits.scoreDocs[0].doc),
                                   hits.scoreDocs[0].doc).toString();

    // Confirm the explanation breaks out the individual
    // sort fields:
    assertTrue(expl, expl.contains("= sort field <int: \"popularity\">! value=20"));

    // Confirm the explanation includes first pass details:
    assertTrue(expl.contains("= first pass score"));
    assertTrue(expl.contains("body:contents in"));
  }

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    int numDocs = atLeast(1000);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final int[] idToNum = new int[numDocs];
    int maxValue = TestUtil.nextInt(random(), 10, 1000000);
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.YES));
      int numTokens = TestUtil.nextInt(random(), 1, 10);
      StringBuilder b = new StringBuilder();
      for(int j=0;j<numTokens;j++) {
        b.append("a ");
      }
      doc.add(newTextField("field", b.toString(), Field.Store.NO));
      idToNum[i] = random().nextInt(maxValue);
      doc.add(new NumericDocValuesField("num", idToNum[i]));
      w.addDocument(doc);
    }
    final IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    int numHits = TestUtil.nextInt(random(), 1, numDocs);
    boolean reverse = random().nextBoolean();

    TopDocs hits = s.search(new TermQuery(new Term("field", "a")), numHits);

    Rescorer rescorer = new SortRescorer(new Sort(new SortField("num", SortField.Type.INT, reverse)));
    TopDocs hits2 = rescorer.rescore(s, hits, numHits);

    Integer[] expected = new Integer[numHits];
    for(int i=0;i<numHits;i++) {
      expected[i] = hits.scoreDocs[i].doc;
    }

    final int reverseInt = reverse ? -1 : 1;

    Arrays.sort(expected,
                new Comparator<Integer>() {
                  @Override
                  public int compare(Integer a, Integer b) {
                    try {
                      int av = idToNum[Integer.parseInt(r.document(a).get("id"))];
                      int bv = idToNum[Integer.parseInt(r.document(b).get("id"))];
                      if (av < bv) {
                        return -reverseInt;
                      } else if (bv < av) {
                        return reverseInt;
                      } else {
                        // Tie break by docID
                        return a - b;
                      }
                    } catch (IOException ioe) {
                      throw new RuntimeException(ioe);
                    }
                  }
                });

    boolean fail = false;
    for(int i=0;i<numHits;i++) {
      fail |= expected[i].intValue() != hits2.scoreDocs[i].doc;
    }
    assertFalse(fail);

    r.close();
    dir.close();
  }
}
