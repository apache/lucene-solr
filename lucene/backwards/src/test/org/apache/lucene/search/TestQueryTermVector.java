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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;

public class TestQueryTermVector extends LuceneTestCase {


  public TestQueryTermVector(String s) {
    super(s);
  }

  public void testConstructor() {
    String [] queryTerm = {"foo", "bar", "foo", "again", "foo", "bar", "go", "go", "go"};
    //Items are sorted lexicographically
    String [] gold = {"again", "bar", "foo", "go"};
    int [] goldFreqs = {1, 2, 3, 3};
    QueryTermVector result = new QueryTermVector(queryTerm);
    assertTrue(result != null);
    String [] terms = result.getTerms();
    assertTrue(terms.length == 4);
    int [] freq = result.getTermFrequencies();
    assertTrue(freq.length == 4);
    checkGold(terms, gold, freq, goldFreqs);
    result = new QueryTermVector(null);
    assertTrue(result.getTerms().length == 0);
    
    result = new QueryTermVector("foo bar foo again foo bar go go go", new WhitespaceAnalyzer());
    assertTrue(result != null);
    terms = result.getTerms();
    assertTrue(terms.length == 4);
    freq = result.getTermFrequencies();
    assertTrue(freq.length == 4);
    checkGold(terms, gold, freq, goldFreqs);
  }

  private void checkGold(String[] terms, String[] gold, int[] freq, int[] goldFreqs) {
    for (int i = 0; i < terms.length; i++) {
      assertTrue(terms[i].equals(gold[i]));
      assertTrue(freq[i] == goldFreqs[i]);
    }
  }
}
