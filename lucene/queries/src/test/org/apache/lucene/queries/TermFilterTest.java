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
package org.apache.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TermFilterTest extends LuceneTestCase {

  public void testCachability() throws Exception {
    TermFilter a = termFilter("field1", "a");
    HashSet<Filter> cachedFilters = new HashSet<>();
    cachedFilters.add(a);
    assertTrue("Must be cached", cachedFilters.contains(termFilter("field1", "a")));
    assertFalse("Must not be cached", cachedFilters.contains(termFilter("field1", "b")));
    assertFalse("Must not be cached", cachedFilters.contains(termFilter("field2", "a")));
  }
  
  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int num = atLeast(100);
    List<Term> terms = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      String field = "field" + i;
      String string = TestUtil.randomRealisticUnicodeString(random());
      terms.add(new Term(field, string));
      Document doc = new Document();
      doc.add(newStringField(field, string, Field.Store.NO));
      w.addDocument(doc);
    }
    IndexReader reader = w.getReader();
    w.close();
    
    IndexSearcher searcher = newSearcher(reader);
    
    int numQueries = atLeast(10);
    for (int i = 0; i < numQueries; i++) {
      Term term = terms.get(random().nextInt(num));
      TopDocs queryResult = searcher.search(new TermQuery(term), reader.maxDoc());
      
      MatchAllDocsQuery matchAll = new MatchAllDocsQuery();
      final TermFilter filter = termFilter(term);
      TopDocs filterResult = searcher.search(matchAll, filter, reader.maxDoc());
      assertEquals(filterResult.totalHits, queryResult.totalHits);
      ScoreDoc[] scoreDocs = filterResult.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) {
        assertEquals(scoreDocs[j].doc, queryResult.scoreDocs[j].doc);
      }
    }
    
    reader.close();
    dir.close();
  }
  
  public void testHashCodeAndEquals() {
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      String field1 = "field" + i;
      String field2 = "field" + i + num;
      String value1 = TestUtil.randomRealisticUnicodeString(random());
      String value2 = value1 + "x"; // this must be not equal to value1

      TermFilter filter1 = termFilter(field1, value1);
      TermFilter filter2 = termFilter(field1, value2);
      TermFilter filter3 = termFilter(field2, value1);
      TermFilter filter4 = termFilter(field2, value2);
      TermFilter[] filters = new TermFilter[]{filter1, filter2, filter3, filter4};
      for (int j = 0; j < filters.length; j++) {
        TermFilter termFilter = filters[j];
        for (int k = 0; k < filters.length; k++) {
          TermFilter otherTermFilter = filters[k];
          if (j == k) {
            assertEquals(termFilter, otherTermFilter);
            assertEquals(termFilter.hashCode(), otherTermFilter.hashCode());
            assertTrue(termFilter.equals(otherTermFilter));
          } else {
            assertFalse(termFilter.equals(otherTermFilter));
          }
        }
      }

      TermFilter filter5 = termFilter(field2, value2);
      assertEquals(filter5, filter4);
      assertEquals(filter5.hashCode(), filter4.hashCode());
      assertTrue(filter5.equals(filter4));

      assertEquals(filter5, filter4);
      assertTrue(filter5.equals(filter4));
    }
  }
  
  public void testToString() {
    TermFilter termsFilter = new TermFilter(new Term("field1", "a"));
    assertEquals("field1:a", termsFilter.toString());
  }

  private TermFilter termFilter(String field, String value) {
    return termFilter(new Term(field, value));
  }

  private TermFilter termFilter(Term term) {
    return new TermFilter(term);
  }

}
