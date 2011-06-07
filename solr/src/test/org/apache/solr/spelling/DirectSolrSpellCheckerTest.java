package org.apache.solr.spelling;

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

import java.util.Collection;
import java.util.Map;

import org.apache.lucene.analysis.Token;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Simple tests for {@link DirectSolrSpellChecker}
 */
public class DirectSolrSpellCheckerTest extends SolrTestCaseJ4 {

  private static SpellingQueryConverter queryConverter;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
    //Index something with a title
    assertNull(h.validateUpdate(adoc("id", "0", "teststop", "This is a title")));
    assertNull(h.validateUpdate(adoc("id", "1", "teststop", "The quick reb fox jumped over the lazy brown dogs.")));
    assertNull(h.validateUpdate(adoc("id", "2", "teststop", "This is a Solr")));
    assertNull(h.validateUpdate(adoc("id", "3", "teststop", "solr foo")));
    assertNull(h.validateUpdate(commit()));
    queryConverter = new SimpleQueryConverter();
    queryConverter.init(new NamedList());
  }
  
  @Test
  public void test() throws Exception {
    DirectSolrSpellChecker checker = new DirectSolrSpellChecker();
    NamedList spellchecker = new NamedList();
    spellchecker.add("classname", DirectSolrSpellChecker.class.getName());
    spellchecker.add(DirectSolrSpellChecker.FIELD, "teststop");
    spellchecker.add(DirectSolrSpellChecker.MINQUERYLENGTH, 2); // we will try "fob"

    SolrCore core = h.getCore();
    checker.init(spellchecker, core);

    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    Collection<Token> tokens = queryConverter.convert("fob");
    SpellingOptions spellOpts = new SpellingOptions(tokens, searcher.get().getIndexReader());
    SpellingResult result = checker.getSuggestions(spellOpts);
    assertTrue("result is null and it shouldn't be", result != null);
    Map<String, Integer> suggestions = result.get(tokens.iterator().next());
    Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
    assertTrue(entry.getKey() + " is not equal to " + "foo", entry.getKey().equals("foo") == true);
    assertFalse(entry.getValue() + " equals: " + SpellingResult.NO_FREQUENCY_INFO, entry.getValue() == SpellingResult.NO_FREQUENCY_INFO);

    spellOpts.tokens = queryConverter.convert("super");
    result = checker.getSuggestions(spellOpts);
    assertTrue("result is null and it shouldn't be", result != null);
    suggestions = result.get(tokens.iterator().next());
    assertTrue("suggestions is not null and it should be", suggestions == null);
    searcher.decref();
  }
}
