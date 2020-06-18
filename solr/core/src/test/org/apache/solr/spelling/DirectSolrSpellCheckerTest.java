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

import java.util.Collection;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.SuppressTempFileChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SpellCheckComponent;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Simple tests for {@link DirectSolrSpellChecker}
 */
@SuppressTempFileChecks(bugUrl = "https://issues.apache.org/jira/browse/SOLR-1877 Spellcheck IndexReader leak bug?")
@SuppressWarnings({"rawtypes"})
public class DirectSolrSpellCheckerTest extends SolrTestCaseJ4 {

  private static SpellingQueryConverter queryConverter;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-spellcheckcomponent.xml","schema.xml");
    //Index something with a title
    assertNull(h.validateUpdate(adoc("id", "0", "teststop", "This is a title")));
    assertNull(h.validateUpdate(adoc("id", "1", "teststop", "The quick reb fox jumped over the lazy brown dogs.")));
    assertNull(h.validateUpdate(adoc("id", "2", "teststop", "This is a Solr")));
    assertNull(h.validateUpdate(adoc("id", "3", "teststop", "solr foo")));
    assertNull(h.validateUpdate(adoc("id", "4", "teststop", "another foo")));
    assertNull(h.validateUpdate(commit()));
    queryConverter = new SimpleQueryConverter();
    queryConverter.init(new NamedList());
  }
  
  @Test
  @SuppressWarnings({"unchecked"})
  public void test() throws Exception {
    DirectSolrSpellChecker checker = new DirectSolrSpellChecker();
    @SuppressWarnings({"rawtypes"})
    NamedList spellchecker = new NamedList();
    spellchecker.add("classname", DirectSolrSpellChecker.class.getName());
    spellchecker.add(SolrSpellChecker.FIELD, "teststop");
    spellchecker.add(DirectSolrSpellChecker.MINQUERYLENGTH, 2); // we will try "fob"

    SolrCore core = h.getCore();
    checker.init(spellchecker, core);

    h.getCore().withSearcher(searcher -> {

      // check that 'fob' is corrected to 'foo'
      Collection<Token> tokens = queryConverter.convert("fob");
      SpellingOptions spellOpts = new SpellingOptions(tokens, searcher.getIndexReader());
      SpellingResult result = checker.getSuggestions(spellOpts);
      assertNotNull("result shouldn't be null", result);
      Map<String, Integer> suggestions = result.get(tokens.iterator().next());
      assertFalse("suggestions shouldn't be empty", suggestions.isEmpty());
      Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
      assertEquals("foo", entry.getKey());
      assertFalse(entry.getValue() + " equals: " + SpellingResult.NO_FREQUENCY_INFO, entry.getValue() == SpellingResult.NO_FREQUENCY_INFO);

      // check that 'super' is *not* corrected
      spellOpts.tokens = queryConverter.convert("super");
      result = checker.getSuggestions(spellOpts);
      assertNotNull("result shouldn't be null", result);
      suggestions = result.get(spellOpts.tokens.iterator().next());
      assertNotNull("suggestions shouldn't be null", suggestions);
      assertTrue("suggestions should be empty", suggestions.isEmpty());
      return null;
    });
  }

  @Test
  public void testOnlyMorePopularWithExtendedResults() throws Exception {
    assertQ(req("q", "teststop:fox", "qt", "/spellCheckCompRH", SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_DICT, "direct", SpellingParams.SPELLCHECK_EXTENDED_RESULTS, "true", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='fox']/int[@name='origFreq']=1",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='fox']/arr[@name='suggestion']/lst/str[@name='word']='foo'",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='fox']/arr[@name='suggestion']/lst/int[@name='freq']=2",
        "//lst[@name='spellcheck']/bool[@name='correctlySpelled']='true'"
    );
  }

  @Test
  public void testMaxQueryLength() throws Exception {
    testMaxQueryLength(true);
    testMaxQueryLength(false);
  }

  private void testMaxQueryLength(Boolean limitQueryLength) throws Exception {

    DirectSolrSpellChecker checker = new DirectSolrSpellChecker();
    NamedList<Object> spellchecker = new NamedList<>();
    spellchecker.add("classname", DirectSolrSpellChecker.class.getName());
    spellchecker.add(SolrSpellChecker.FIELD, "teststop");
    spellchecker.add(DirectSolrSpellChecker.MINQUERYLENGTH, 2);

    // demonstrate that "anothar" is not corrected when maxQueryLength is set to a small number
    if (limitQueryLength) spellchecker.add(DirectSolrSpellChecker.MAXQUERYLENGTH, 4);

    SolrCore core = h.getCore();
    checker.init(spellchecker, core);

    h.getCore().withSearcher(searcher -> {
      Collection<Token> tokens = queryConverter.convert("anothar");
      SpellingOptions spellOpts = new SpellingOptions(tokens, searcher.getIndexReader());
      SpellingResult result = checker.getSuggestions(spellOpts);
      assertNotNull("result shouldn't be null", result);
      Map<String, Integer> suggestions = result.get(tokens.iterator().next());
      assertNotNull("suggestions shouldn't be null", suggestions);

      if (limitQueryLength) {
        assertTrue("suggestions should be empty", suggestions.isEmpty());
      } else {
        assertFalse("suggestions shouldn't be empty", suggestions.isEmpty());
        Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
        assertEquals("another", entry.getKey());
      }

      return null;
    });
  }
  
}
