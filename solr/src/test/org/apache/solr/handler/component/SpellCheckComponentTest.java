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

package org.apache.solr.handler.component;

import java.io.File;
import java.util.*;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.spelling.AbstractLuceneSpellChecker;
import org.apache.solr.spelling.IndexBasedSpellChecker;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @since solr 1.3
 */
public class SpellCheckComponentTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
    assertNull(h.validateUpdate(adoc("id", "0", "lowerfilt", "This is a title")));
    assertNull(h.validateUpdate(adoc("id", "1", "lowerfilt",
            "The quick reb fox jumped over the lazy brown dogs.")));
    assertNull(h.validateUpdate(adoc("id", "2", "lowerfilt", "This is a document")));
    assertNull(h.validateUpdate(adoc("id", "3", "lowerfilt", "another document")));
    //bunch of docs that are variants on blue
    assertNull(h.validateUpdate(adoc("id", "4", "lowerfilt", "blue")));
    assertNull(h.validateUpdate(adoc("id", "5", "lowerfilt", "blud")));
    assertNull(h.validateUpdate(adoc("id", "6", "lowerfilt", "boue")));
    assertNull(h.validateUpdate(adoc("id", "7", "lowerfilt", "glue")));
    assertNull(h.validateUpdate(adoc("id", "8", "lowerfilt", "blee")));
    assertNull(h.validateUpdate(commit()));
  }
  
  @Test
  public void testExtendedResultsCount() throws Exception {
    SolrCore core = h.getCore();
    SearchComponent speller = core.getSearchComponent("spellcheck");
    assertTrue("speller is null and it shouldn't be", speller != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.QT, "spellCheckCompRH");
    params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
    params.add(CommonParams.Q, "bluo");
    params.add(SpellCheckComponent.COMPONENT_NAME, "true");
    params.add(SpellCheckComponent.SPELLCHECK_COUNT, String.valueOf(5));
    params.add(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, String.valueOf(false));
    SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
    SolrQueryResponse rsp;
    rsp = new SolrQueryResponse();
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();
    String cmdExec = (String) values.get("command");
    assertEquals("build",cmdExec);
    NamedList spellCheck = (NamedList) values.get("spellcheck");
    NamedList suggestions = (NamedList) spellCheck.get("suggestions");
    NamedList blue = (NamedList) suggestions.get("bluo");
    assertEquals(5,blue.get("numFound"));
    Collection<String> theSuggestion = (Collection<String>) blue.get("suggestion");
    assertEquals(5,theSuggestion.size());
    //we know there are at least 5, but now only get 3

    params.remove(SpellCheckComponent.SPELLCHECK_COUNT);
    params.remove(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS);
    params.remove(SpellCheckComponent.SPELLCHECK_BUILD);
    params.add(SpellCheckComponent.SPELLCHECK_COUNT, "3");
    params.add(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, String.valueOf(true));
    params.add(SpellCheckComponent.SPELLCHECK_BUILD, "false");
    rsp = new SolrQueryResponse();
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();

    spellCheck = (NamedList) values.get("spellcheck");
    suggestions = (NamedList) spellCheck.get("suggestions");
    blue = (NamedList) suggestions.get("bluo");
    assertEquals(3, blue.get("numFound"));

    List<SimpleOrderedMap> theSuggestions = (List<SimpleOrderedMap>)blue.get("suggestion");
    assertEquals(3, theSuggestions.size());

    for (SimpleOrderedMap sug : theSuggestions) {
      assertNotNull(sug.get("word"));
      assertNotNull(sug.get("freq"));      
    }
  }

  @Test
  public void test() throws Exception {
    SolrCore core = h.getCore();
    SearchComponent speller = core.getSearchComponent("spellcheck");
    assertTrue("speller is null and it shouldn't be", speller != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.QT, "spellCheckCompRH");
    params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
    params.add(CommonParams.Q, "documemt");
    params.add(SpellCheckComponent.COMPONENT_NAME, "true");

    SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();
    String cmdExec = (String) values.get("command");
    assertTrue("command is null and it shouldn't be", cmdExec != null);
    assertTrue(cmdExec + " is not equal to " + "build",
            cmdExec.equals("build") == true);
    NamedList spellCheck = (NamedList) values.get("spellcheck");
    assertNotNull(spellCheck);
    NamedList suggestions = (NamedList) spellCheck.get("suggestions");
    assertNotNull(suggestions);
    NamedList document = (NamedList) suggestions.get("documemt");
    assertEquals(1, document.get("numFound"));
    assertEquals(0, document.get("startOffset"));
    assertEquals(document.get("endOffset"), "documemt".length());
    Collection<String> theSuggestion = (Collection<String>) document.get("suggestion");
    assertEquals(1, theSuggestion.size());
    assertEquals("document", theSuggestion.iterator().next());
  }


  @Test
  public void testPerDictionary() throws Exception {
    SolrCore core = h.getCore();
    SearchComponent speller = core.getSearchComponent("spellcheck");
    assertTrue("speller is null and it shouldn't be", speller != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.QT, "spellCheckCompRH");
    params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
    params.add(CommonParams.Q, "documemt");
    params.add(SpellCheckComponent.COMPONENT_NAME, "true");
    params.add(SpellingParams.SPELLCHECK_DICT, "perDict");

    params.add(SpellingParams.SPELLCHECK_PREFIX + ".perDict.foo", "bar");
    params.add(SpellingParams.SPELLCHECK_PREFIX + ".perDict.bar", "foo");

    SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();

    NamedList spellCheck = (NamedList) values.get("spellcheck");
    NamedList suggestions = (NamedList) spellCheck.get("suggestions");
    assertNotNull("suggestions", suggestions);
    NamedList suggestion;
    Collection<String> theSuggestion;
    suggestion = (NamedList) suggestions.get("foo");
    assertEquals(1, suggestion.get("numFound"));
    assertEquals(0, suggestion.get("startOffset"));
    assertEquals(suggestion.get("endOffset"), 1);
    theSuggestion = (Collection<String>) suggestion.get("suggestion");
    assertEquals(1, theSuggestion.size());
    assertEquals("bar", theSuggestion.iterator().next());

    suggestion = (NamedList) suggestions.get("bar");
    assertEquals(1, suggestion.get("numFound"));
    assertEquals(2, suggestion.get("startOffset"));
    assertEquals(3, suggestion.get("endOffset"));
    theSuggestion = (Collection<String>) suggestion.get("suggestion");
    assertEquals(1, theSuggestion.size());
    assertEquals("foo", theSuggestion.iterator().next());
  }

  @Test
  public void testCollate() throws Exception {
    SolrCore core = h.getCore();
    SearchComponent speller = core.getSearchComponent("spellcheck");
    assertTrue("speller is null and it shouldn't be", speller != null);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.QT, "spellCheckCompRH");
    params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
    params.add(CommonParams.Q, "documemt");
    params.add(SpellCheckComponent.COMPONENT_NAME, "true");
    params.add(SpellCheckComponent.SPELLCHECK_COLLATE, "true");

    SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();
    NamedList spellCheck = (NamedList) values.get("spellcheck");
    NamedList suggestions = (NamedList) spellCheck.get("suggestions");
    String collation = (String) suggestions.get("collation");
    assertEquals("document", collation);
    params.remove(CommonParams.Q);
    params.add(CommonParams.Q, "documemt lowerfilt:broen^4");
    handler = core.getRequestHandler("spellCheckCompRH");
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    spellCheck = (NamedList) values.get("spellcheck");
    suggestions = (NamedList) spellCheck.get("suggestions");
    collation = (String) suggestions.get("collation");
    assertEquals("document lowerfilt:brown^4", collation);

    params.remove(CommonParams.Q);
    params.add(CommonParams.Q, "documemtsss broens");
    handler = core.getRequestHandler("spellCheckCompRH");
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    spellCheck = (NamedList) values.get("spellcheck");
    suggestions = (NamedList) spellCheck.get("suggestions");
    collation = (String) suggestions.get("collation");
    assertEquals("document brown",collation);
  }

  @Test
  public void testCorrectSpelling() throws Exception {
    SolrCore core = h.getCore();
    Map<String, String> args = new HashMap<String, String>();

    args.put(CommonParams.Q, "lowerfilt:lazy lowerfilt:brown");
    args.put(CommonParams.QT, "spellCheckCompRH");
    args.put(SpellCheckComponent.SPELLCHECK_BUILD, "true");
    args.put(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true");
    args.put(SpellCheckComponent.COMPONENT_NAME, "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(
            args));

    assertQ("Make sure correct spellings are signalled in the response", req, 
            "//*[@numFound='1']", "//result/doc[1]/int[@name='id'][.='1']",
            "//*/lst[@name='suggestions']");
    
    
    args = new HashMap<String, String>();

    args.put(CommonParams.Q, "lakkle");
    args.put(CommonParams.QT, "spellCheckCompRH");
    args.put(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true");
    args.put(SpellCheckComponent.COMPONENT_NAME, "true");
    req = new LocalSolrQueryRequest(core, new MapSolrParams(
            args));
    
    assertQ("Make sure correct spellings are signalled in the response", req, 
        "//*[@numFound='0']", "//*/lst[@name='suggestions']", "//*/bool[@name='correctlySpelled'][.='false']");
    
    
    args = new HashMap<String, String>();

    args.put(CommonParams.Q, "lowerfilt:lazy");
    args.put(CommonParams.QT, "spellCheckCompRH");
    args.put(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true");
    args.put(SpellCheckComponent.COMPONENT_NAME, "true");
    req = new LocalSolrQueryRequest(core, new MapSolrParams(
            args));
    
    assertQ("Make sure correct spellings are signalled in the response", req, 
        "//*[@numFound='1']", "//*/lst[@name='suggestions']", "//*/bool[@name='correctlySpelled'][.='true']");
  }

  @Test
  public void testInit() throws Exception {
    SolrCore core = h.getCore();
    SpellCheckComponent scc = new SpellCheckComponent();
    NamedList args = new NamedList();
    NamedList spellchecker = new NamedList();
    spellchecker.add("classname", IndexBasedSpellChecker.class.getName());
    spellchecker.add("name", "default");
    spellchecker.add("field", "lowerfilt");
    spellchecker.add("spellcheckIndexDir", "./spellchecker");

    args.add("spellchecker", spellchecker);
    NamedList altSC = new NamedList();
    altSC.add("classname", IndexBasedSpellChecker.class.getName());
    altSC.add("name", "alternate");
    altSC.add("field", "lowerfilt");
    altSC.add("spellcheckIndexDir", "./spellchecker");

    args.add("spellchecker", altSC);
    args.add("queryAnalyzerFieldType", "lowerfilt");
    NamedList defaults = new NamedList();
    defaults.add(SpellCheckComponent.SPELLCHECK_COLLATE, true);
    defaults.add(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, false);
    defaults.add(SpellCheckComponent.SPELLCHECK_COUNT, 2);
    args.add("defaults", defaults);
    scc.init(args);
    scc.inform(core);
    //hmm, not sure what to assert here...

    //add the sc again and then init again, we should get an exception
    args.add("spellchecker", spellchecker);
    scc = new SpellCheckComponent();
    scc.init(args);
    try {
      scc.inform(core);
      assertTrue(false);
    } catch (Exception e) {

    }


  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testRelativeIndexDirLocation() throws Exception {
    SolrCore core = h.getCore();
    Map<String, String> args = new HashMap<String, String>();

    args.put(CommonParams.Q, "test");
    args.put(CommonParams.QT, "spellCheckCompRH");
    args.put(SpellCheckComponent.SPELLCHECK_BUILD, "true");
    args.put(SpellCheckComponent.COMPONENT_NAME, "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(
        args));

    File indexDir = new File(core.getDataDir() + File.separator
        + "spellchecker1");
    assertTrue(
        "spellcheckerIndexDir was not created inside the configured value for dataDir folder as configured in solrconfig.xml",
        indexDir.exists());
    
    indexDir = new File(core.getDataDir() + File.separator
        + "spellchecker2");
    assertTrue(
        "spellcheckerIndexDir was not created inside the configured value for dataDir folder as configured in solrconfig.xml",
        indexDir.exists());
    
    indexDir = new File(core.getDataDir() + File.separator
        + "spellchecker3");
    assertTrue(
        "spellcheckerIndexDir was not created inside the configured value for dataDir folder as configured in solrconfig.xml",
        indexDir.exists());
  }

  @Test
  public void testReloadOnStart() throws Exception {
    assertU(adoc("id", "0", "lowerfilt", "This is a title"));
    assertU(commit());
    SolrQueryRequest request = req("qt", "spellCheckCompRH", "q", "*:*",
        "spellcheck.q", "ttle", "spellcheck", "true", "spellcheck.dictionary",
        "default", "spellcheck.build", "true");
    assertQ(request, "//arr[@name='suggestion'][.='title']");

    NamedList args = new NamedList();
    NamedList spellchecker = new NamedList();
    spellchecker.add(AbstractLuceneSpellChecker.DICTIONARY_NAME, "default");
    spellchecker.add(AbstractLuceneSpellChecker.FIELD, "lowerfilt");
    spellchecker.add(AbstractLuceneSpellChecker.INDEX_DIR, "spellchecker1");
    args.add("spellchecker", spellchecker);

    SpellCheckComponent checker = new SpellCheckComponent();
    checker.init(args);
    checker.inform(h.getCore());

    request = req("qt", "spellCheckCompRH", "q", "*:*", "spellcheck.q", "ttle",
        "spellcheck", "true", "spellcheck.dictionary", "default",
        "spellcheck.reload", "true");
    ResponseBuilder rb = new ResponseBuilder();
    rb.req = request;
    rb.rsp = new SolrQueryResponse();
    rb.components = new ArrayList(h.getCore().getSearchComponents().values());
    checker.prepare(rb);

    try {
      checker.process(rb);
    } catch (NullPointerException e) {
      fail("NullPointerException due to reload not initializing analyzers");
    }
  }
  
    @SuppressWarnings("unchecked")
    @Test
  public void testRebuildOnCommit() throws Exception {
    SolrQueryRequest req = req("q", "lowerfilt:lucenejavt", "qt", "spellCheckCompRH", "spellcheck", "true");
    String response = h.query(req);
    assertFalse("No suggestions should be returned", response.contains("lucenejava"));
    
    assertU(adoc("id", "11231", "lowerfilt", "lucenejava"));
    assertU("commit", commit());
    
    assertQ(req, "//arr[@name='suggestion'][.='lucenejava']");
  }
  
  // TODO: add more tests for various spelling options

}
