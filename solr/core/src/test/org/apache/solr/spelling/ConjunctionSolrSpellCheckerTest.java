package org.apache.solr.spelling;

import java.io.IOException;

import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.Test;

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

public class ConjunctionSolrSpellCheckerTest extends LuceneTestCase {
  
  @Test
  public void test() throws Exception {
    ConjunctionSolrSpellChecker cssc = new ConjunctionSolrSpellChecker();
    MockSolrSpellChecker levenstein1 = new MockSolrSpellChecker(new LevensteinDistance());
    MockSolrSpellChecker levenstein2 = new MockSolrSpellChecker(new LevensteinDistance());
    MockSolrSpellChecker ngram = new MockSolrSpellChecker(new NGramDistance());
    
    cssc.addChecker(levenstein1);
    cssc.addChecker(levenstein2);
    try {
      cssc.addChecker(ngram);
      fail("ConjunctionSolrSpellChecker should have thrown an exception about non-identical StringDistances.");
    } catch (IllegalArgumentException iae) {
      // correct behavior
    }
  }
  
  class MockSolrSpellChecker extends SolrSpellChecker {
    
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
