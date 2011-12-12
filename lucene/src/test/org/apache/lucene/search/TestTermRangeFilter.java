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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
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
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, maxIP, T, T),
        numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, maxIP, T, F),
        numDocs).scoreDocs;
    assertEquals("all but last", numDocs - 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, maxIP, F, T),
        numDocs).scoreDocs;
    assertEquals("all but first", numDocs - 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, maxIP, F, F),
        numDocs).scoreDocs;
    assertEquals("all but ends", numDocs - 2, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", medIP, maxIP, T, T),
        numDocs).scoreDocs;
    assertEquals("med and up", 1 + maxId - medId, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, medIP, T, T),
        numDocs).scoreDocs;
    assertEquals("up to med", 1 + medId - minId, result.length);
    
    // unbounded id
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("min and up", numDocs, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", null, maxIP, F, T),
        numDocs).scoreDocs;
    assertEquals("max and down", numDocs, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, null, F, F),
        numDocs).scoreDocs;
    assertEquals("not min, but up", numDocs - 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", null, maxIP, F, F),
        numDocs).scoreDocs;
    assertEquals("not max, but down", numDocs - 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", medIP, maxIP, T, F),
        numDocs).scoreDocs;
    assertEquals("med and up, not max", maxId - medId, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, medIP, F, T),
        numDocs).scoreDocs;
    assertEquals("not min, up to med", medId - minId, result.length);
    
    // very small sets
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, minIP, F, F),
        numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q, TermRangeFilter.newStringRange("id", medIP, medIP, F, F),
        numDocs).scoreDocs;
    assertEquals("med,med,F,F", 0, result.length);
    result = search.search(q, TermRangeFilter.newStringRange("id", maxIP, maxIP, F, F),
        numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", minIP, minIP, T, T),
        numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q, TermRangeFilter.newStringRange("id", null, minIP, F, T),
        numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", maxIP, maxIP, T, T),
        numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q, TermRangeFilter.newStringRange("id", maxIP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("id", medIP, medIP, T, T),
        numDocs).scoreDocs;
    assertEquals("med,med,T,T", 1, result.length);
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
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, maxRP, T, T),
        numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, maxRP, T, F),
        numDocs).scoreDocs;
    assertEquals("all but biggest", numDocs - 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, maxRP, F, T),
        numDocs).scoreDocs;
    assertEquals("all but smallest", numDocs - 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, maxRP, F, F),
        numDocs).scoreDocs;
    assertEquals("all but extremes", numDocs - 2, result.length);
    
    // unbounded
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("smallest and up", numDocs, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", null, maxRP, F, T),
        numDocs).scoreDocs;
    assertEquals("biggest and down", numDocs, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, null, F, F),
        numDocs).scoreDocs;
    assertEquals("not smallest, but up", numDocs - 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", null, maxRP, F, F),
        numDocs).scoreDocs;
    assertEquals("not biggest, but down", numDocs - 1, result.length);
    
    // very small sets
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, minRP, F, F),
        numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q, TermRangeFilter.newStringRange("rand", maxRP, maxRP, F, F),
        numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", minRP, minRP, T, T),
        numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q, TermRangeFilter.newStringRange("rand", null, minRP, F, T),
        numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);
    
    result = search.search(q, TermRangeFilter.newStringRange("rand", maxRP, maxRP, T, T),
        numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q, TermRangeFilter.newStringRange("rand", maxRP, null, T, F),
        numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);
  }
}
