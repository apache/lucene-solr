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
package org.apache.solr.spelling;

import java.io.IOException;

import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.LuceneLevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.solr.SolrTestCase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.Assert;
import org.junit.Test;

public class ConjunctionSolrSpellCheckerTest extends SolrTestCase {
  
  public static final Class<?>[] AVAILABLE_DISTANCES = {LevenshteinDistance.class, LuceneLevenshteinDistance.class,
      JaroWinklerDistance.class, NGramDistance.class};

  @Test
  public void test() throws Exception {
    ConjunctionSolrSpellChecker cssc = new ConjunctionSolrSpellChecker();
    @SuppressWarnings("unchecked")
    Class<StringDistance> sameDistance = (Class<StringDistance>) AVAILABLE_DISTANCES[random().nextInt(AVAILABLE_DISTANCES.length)];
    
    StringDistance sameDistance1 = sameDistance.getConstructor().newInstance();
    StringDistance sameDistance2 = sameDistance.getConstructor().newInstance();
    
    //NGramDistance defaults to 2, so we'll try 3 or 4 to ensure we have one that is not-equal.
    StringDistance differentDistance = new NGramDistance(3);
    if(sameDistance1.equals(differentDistance)) {
      differentDistance = new NGramDistance(4);
      if(sameDistance1.equals(differentDistance)) {
        fail("Cannot set up test.  2 NGramDistances with different gram sizes should not be equal.");
      }
    }
    Assert.assertEquals("The distance " + sameDistance + " does not properly implement equals.", sameDistance1, sameDistance2);
    
    
    MockSolrSpellChecker checker1 = new MockSolrSpellChecker(sameDistance1);
    MockSolrSpellChecker checker2 = new MockSolrSpellChecker(sameDistance2);
    MockSolrSpellChecker checker3 = new MockSolrSpellChecker(differentDistance);
    
    cssc.addChecker(checker1);
    cssc.addChecker(checker2);
    expectThrows(IllegalArgumentException.class, () -> cssc.addChecker(checker3));
  }

  static class MockSolrSpellChecker extends SolrSpellChecker {
    
    final StringDistance sd;
    
    MockSolrSpellChecker(StringDistance sd) {
      this.sd = sd;
    }
    
    @Override
    protected StringDistance getStringDistance() {
      return sd;
    }
    
    @Override
    public void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException {}
    
    @Override
    public void build(SolrCore core, SolrIndexSearcher searcher) throws IOException {}
    
    @Override
    public SpellingResult getSuggestions(SpellingOptions options) throws IOException {
      return null;
    }
    
  }
}
