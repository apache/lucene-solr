package org.apache.lucene.search;

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
import java.text.Collator;
import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.junit.Test;

/**
 * A basic 'positive' Unit test class for the TermRangeFilter class.
 * 
 * <p>
 * NOTE: at the moment, this class only tests for 'positive' results, it does
 * not verify the results to ensure there are no 'false positives', nor does it
 * adequately test 'negative' results. It also does not test that garbage in
 * results in an Exception.
 */
public class TestTermRangeFilter extends BaseTestRangeFilter {
  
  @Test
  public void testRangeFilterId() throws IOException {
    
    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);
    
    int medId = ((maxId - minId) / 2);
    
    String minIP = pad(minId);
    String maxIP = pad(maxId);
    String medIP = pad(medId);
    
    int numDocs = reader.numDocs();
    
    assertEquals("num of docs", numDocs, 1 + maxId - minId);
    
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body", "body"));
    
    // test id, bounded on both ends
    
    result = search.search(q, new TermRangeFilter("id", minIP, maxIP, T, T),
        numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);
    
    result = search.search(q, new TermRangeFilter("id", minIP, maxIP, T, F),
        numDocs).scoreDocs;
    assertEquals("all but last", numDocs - 1, result.length);
    
    result = search.search(q, new TermRangeFilter("id", minIP, maxIP, F, T),
        numDocs).scoreDocs;
    assertEquals("all but first", numDocs - 1, result.length);
    
    result = search.search(q, new TermRangeFilter("id", minIP, maxIP, F, F),
        numDocs).scoreDocs;
    assertEquals("all but ends", numDocs - 2, result.length);
    
    result = search.search(q, new TermRangeFilter("id", medIP, maxIP, T, T),
        numDocs).scoreDocs;
    assertEquals("med and up", 1 + maxId - medId, result.length);
    
    result = search.search(q, new TermRangeFilter("id", minIP, medIP, T, T),
        numDocs).scoreDocs;
    assertEquals("up to med", 1 + medId - minId, result.length);
    
    // unbounded id
    
    result = search.search(q, new TermRangeFilter("id", minIP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("min and up", numDocs, result.length);
    
    result = search.search(q, new TermRangeFilter("id", null, maxIP, F, T),
        numDocs).scoreDocs;
    assertEquals("max and down", numDocs, result.length);
    
    result = search.search(q, new TermRangeFilter("id", minIP, null, F, F),
        numDocs).scoreDocs;
    assertEquals("not min, but up", numDocs - 1, result.length);
    
    result = search.search(q, new TermRangeFilter("id", null, maxIP, F, F),
        numDocs).scoreDocs;
    assertEquals("not max, but down", numDocs - 1, result.length);
    
    result = search.search(q, new TermRangeFilter("id", medIP, maxIP, T, F),
        numDocs).scoreDocs;
    assertEquals("med and up, not max", maxId - medId, result.length);
    
    result = search.search(q, new TermRangeFilter("id", minIP, medIP, F, T),
        numDocs).scoreDocs;
    assertEquals("not min, up to med", medId - minId, result.length);
    
    // very small sets
    
    result = search.search(q, new TermRangeFilter("id", minIP, minIP, F, F),
        numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q, new TermRangeFilter("id", medIP, medIP, F, F),
        numDocs).scoreDocs;
    assertEquals("med,med,F,F", 0, result.length);
    result = search.search(q, new TermRangeFilter("id", maxIP, maxIP, F, F),
        numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
    
    result = search.search(q, new TermRangeFilter("id", minIP, minIP, T, T),
        numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q, new TermRangeFilter("id", null, minIP, F, T),
        numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);
    
    result = search.search(q, new TermRangeFilter("id", maxIP, maxIP, T, T),
        numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q, new TermRangeFilter("id", maxIP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);
    
    result = search.search(q, new TermRangeFilter("id", medIP, medIP, T, T),
        numDocs).scoreDocs;
    assertEquals("med,med,T,T", 1, result.length);
    
    search.close();
  }
  
  @Test
  public void testRangeFilterIdCollating() throws IOException {
    
    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);
    
    Collator c = Collator.getInstance(Locale.ENGLISH);
    
    int medId = ((maxId - minId) / 2);
    
    String minIP = pad(minId);
    String maxIP = pad(maxId);
    String medIP = pad(medId);
    
    int numDocs = reader.numDocs();
    
    assertEquals("num of docs", numDocs, 1 + maxId - minId);
    
    Query q = new TermQuery(new Term("body", "body"));
    
    // test id, bounded on both ends
    int numHits = search.search(q, new TermRangeFilter("id", minIP, maxIP, T,
        T, c), 1000).totalHits;
    assertEquals("find all", numDocs, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", minIP, maxIP, T, F, c), 1000).totalHits;
    assertEquals("all but last", numDocs - 1, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", minIP, maxIP, F, T, c), 1000).totalHits;
    assertEquals("all but first", numDocs - 1, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", minIP, maxIP, F, F, c), 1000).totalHits;
    assertEquals("all but ends", numDocs - 2, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", medIP, maxIP, T, T, c), 1000).totalHits;
    assertEquals("med and up", 1 + maxId - medId, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", minIP, medIP, T, T, c), 1000).totalHits;
    assertEquals("up to med", 1 + medId - minId, numHits);
    
    // unbounded id
    
    numHits = search.search(q, new TermRangeFilter("id", minIP, null, T, F, c),
        1000).totalHits;
    assertEquals("min and up", numDocs, numHits);
    
    numHits = search.search(q, new TermRangeFilter("id", null, maxIP, F, T, c),
        1000).totalHits;
    assertEquals("max and down", numDocs, numHits);
    
    numHits = search.search(q, new TermRangeFilter("id", minIP, null, F, F, c),
        1000).totalHits;
    assertEquals("not min, but up", numDocs - 1, numHits);
    
    numHits = search.search(q, new TermRangeFilter("id", null, maxIP, F, F, c),
        1000).totalHits;
    assertEquals("not max, but down", numDocs - 1, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", medIP, maxIP, T, F, c), 1000).totalHits;
    assertEquals("med and up, not max", maxId - medId, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", minIP, medIP, F, T, c), 1000).totalHits;
    assertEquals("not min, up to med", medId - minId, numHits);
    
    // very small sets
    
    numHits = search.search(q,
        new TermRangeFilter("id", minIP, minIP, F, F, c), 1000).totalHits;
    assertEquals("min,min,F,F", 0, numHits);
    numHits = search.search(q,
        new TermRangeFilter("id", medIP, medIP, F, F, c), 1000).totalHits;
    assertEquals("med,med,F,F", 0, numHits);
    numHits = search.search(q,
        new TermRangeFilter("id", maxIP, maxIP, F, F, c), 1000).totalHits;
    assertEquals("max,max,F,F", 0, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", minIP, minIP, T, T, c), 1000).totalHits;
    assertEquals("min,min,T,T", 1, numHits);
    numHits = search.search(q, new TermRangeFilter("id", null, minIP, F, T, c),
        1000).totalHits;
    assertEquals("nul,min,F,T", 1, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", maxIP, maxIP, T, T, c), 1000).totalHits;
    assertEquals("max,max,T,T", 1, numHits);
    numHits = search.search(q, new TermRangeFilter("id", maxIP, null, T, F, c),
        1000).totalHits;
    assertEquals("max,nul,T,T", 1, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("id", medIP, medIP, T, T, c), 1000).totalHits;
    assertEquals("med,med,T,T", 1, numHits);
    
    search.close();
  }
  
  @Test
  public void testRangeFilterRand() throws IOException {
    
    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);
    
    String minRP = pad(signedIndexDir.minR);
    String maxRP = pad(signedIndexDir.maxR);
    
    int numDocs = reader.numDocs();
    
    assertEquals("num of docs", numDocs, 1 + maxId - minId);
    
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body", "body"));
    
    // test extremes, bounded on both ends
    
    result = search.search(q, new TermRangeFilter("rand", minRP, maxRP, T, T),
        numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", minRP, maxRP, T, F),
        numDocs).scoreDocs;
    assertEquals("all but biggest", numDocs - 1, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", minRP, maxRP, F, T),
        numDocs).scoreDocs;
    assertEquals("all but smallest", numDocs - 1, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", minRP, maxRP, F, F),
        numDocs).scoreDocs;
    assertEquals("all but extremes", numDocs - 2, result.length);
    
    // unbounded
    
    result = search.search(q, new TermRangeFilter("rand", minRP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("smallest and up", numDocs, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", null, maxRP, F, T),
        numDocs).scoreDocs;
    assertEquals("biggest and down", numDocs, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", minRP, null, F, F),
        numDocs).scoreDocs;
    assertEquals("not smallest, but up", numDocs - 1, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", null, maxRP, F, F),
        numDocs).scoreDocs;
    assertEquals("not biggest, but down", numDocs - 1, result.length);
    
    // very small sets
    
    result = search.search(q, new TermRangeFilter("rand", minRP, minRP, F, F),
        numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q, new TermRangeFilter("rand", maxRP, maxRP, F, F),
        numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", minRP, minRP, T, T),
        numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q, new TermRangeFilter("rand", null, minRP, F, T),
        numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);
    
    result = search.search(q, new TermRangeFilter("rand", maxRP, maxRP, T, T),
        numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q, new TermRangeFilter("rand", maxRP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);
    
    search.close();
  }
  
  @Test
  public void testRangeFilterRandCollating() throws IOException {
    
    // using the unsigned index because collation seems to ignore hyphens
    IndexReader reader = unsignedIndexReader;
    IndexSearcher search = newSearcher(reader);
    
    Collator c = Collator.getInstance(Locale.ENGLISH);
    
    String minRP = pad(unsignedIndexDir.minR);
    String maxRP = pad(unsignedIndexDir.maxR);
    
    int numDocs = reader.numDocs();
    
    assertEquals("num of docs", numDocs, 1 + maxId - minId);
    
    Query q = new TermQuery(new Term("body", "body"));
    
    // test extremes, bounded on both ends
    
    int numHits = search.search(q, new TermRangeFilter("rand", minRP, maxRP, T,
        T, c), 1000).totalHits;
    assertEquals("find all", numDocs, numHits);
    
    numHits = search.search(q, new TermRangeFilter("rand", minRP, maxRP, T, F,
        c), 1000).totalHits;
    assertEquals("all but biggest", numDocs - 1, numHits);
    
    numHits = search.search(q, new TermRangeFilter("rand", minRP, maxRP, F, T,
        c), 1000).totalHits;
    assertEquals("all but smallest", numDocs - 1, numHits);
    
    numHits = search.search(q, new TermRangeFilter("rand", minRP, maxRP, F, F,
        c), 1000).totalHits;
    assertEquals("all but extremes", numDocs - 2, numHits);
    
    // unbounded
    
    numHits = search.search(q,
        new TermRangeFilter("rand", minRP, null, T, F, c), 1000).totalHits;
    assertEquals("smallest and up", numDocs, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("rand", null, maxRP, F, T, c), 1000).totalHits;
    assertEquals("biggest and down", numDocs, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("rand", minRP, null, F, F, c), 1000).totalHits;
    assertEquals("not smallest, but up", numDocs - 1, numHits);
    
    numHits = search.search(q,
        new TermRangeFilter("rand", null, maxRP, F, F, c), 1000).totalHits;
    assertEquals("not biggest, but down", numDocs - 1, numHits);
    
    // very small sets
    
    numHits = search.search(q, new TermRangeFilter("rand", minRP, minRP, F, F,
        c), 1000).totalHits;
    assertEquals("min,min,F,F", 0, numHits);
    numHits = search.search(q, new TermRangeFilter("rand", maxRP, maxRP, F, F,
        c), 1000).totalHits;
    assertEquals("max,max,F,F", 0, numHits);
    
    numHits = search.search(q, new TermRangeFilter("rand", minRP, minRP, T, T,
        c), 1000).totalHits;
    assertEquals("min,min,T,T", 1, numHits);
    numHits = search.search(q,
        new TermRangeFilter("rand", null, minRP, F, T, c), 1000).totalHits;
    assertEquals("nul,min,F,T", 1, numHits);
    
    numHits = search.search(q, new TermRangeFilter("rand", maxRP, maxRP, T, T,
        c), 1000).totalHits;
    assertEquals("max,max,T,T", 1, numHits);
    numHits = search.search(q,
        new TermRangeFilter("rand", maxRP, null, T, F, c), 1000).totalHits;
    assertEquals("max,nul,T,T", 1, numHits);
    
    search.close();
  }
  
  @Test
  public void testFarsi() throws Exception {
    
    /* build an index */
    Directory farsiIndex = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, farsiIndex);
    Document doc = new Document();
    doc.add(newField("content", "\u0633\u0627\u0628", Field.Store.YES,
        Field.Index.NOT_ANALYZED));
    doc
        .add(newField("body", "body", Field.Store.YES,
            Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher search = newSearcher(reader);
    Query q = new TermQuery(new Term("body", "body"));
    
    // Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
    // RuleBasedCollator. However, the Arabic Locale seems to order the Farsi
    // characters properly.
    Collator collator = Collator.getInstance(new Locale("ar"));
    
    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a TermRangeFilter with a Farsi
    // Collator (or an Arabic one for the case when Farsi is not supported).
    int numHits = search.search(q, new TermRangeFilter("content", "\u062F",
        "\u0698", T, T, collator), 1000).totalHits;
    assertEquals("The index Term should not be included.", 0, numHits);
    
    numHits = search.search(q, new TermRangeFilter("content", "\u0633",
        "\u0638", T, T, collator), 1000).totalHits;
    assertEquals("The index Term should be included.", 1, numHits);
    search.close();
    reader.close();
    farsiIndex.close();
  }
  
  @Test
  public void testDanish() throws Exception {
    
    /* build an index */
    Directory danishIndex = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, danishIndex);
    // Danish collation orders the words below in the given order
    // (example taken from TestSort.testInternationalSort() ).
    String[] words = {"H\u00D8T", "H\u00C5T", "MAND"};
    for (int docnum = 0; docnum < words.length; ++docnum) {
      Document doc = new Document();
      doc.add(newField("content", words[docnum], Field.Store.YES,
          Field.Index.NOT_ANALYZED));
      doc.add(newField("body", "body", Field.Store.YES,
          Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher search = newSearcher(reader);
    Query q = new TermQuery(new Term("body", "body"));
    
    Collator collator = Collator.getInstance(new Locale("da", "dk"));
    
    // Unicode order would not include "H\u00C5T" in [ "H\u00D8T", "MAND" ],
    // but Danish collation does.
    int numHits = search.search(q, new TermRangeFilter("content", "H\u00D8T",
        "MAND", F, F, collator), 1000).totalHits;
    assertEquals("The index Term should be included.", 1, numHits);
    
    numHits = search.search(q, new TermRangeFilter("content", "H\u00C5T",
        "MAND", F, F, collator), 1000).totalHits;
    assertEquals("The index Term should not be included.", 0, numHits);
    search.close();
    reader.close();
    danishIndex.close();
  }
}
