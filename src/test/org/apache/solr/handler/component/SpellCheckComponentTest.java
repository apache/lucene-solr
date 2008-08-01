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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.spelling.AbstractLuceneSpellChecker;
import org.apache.solr.spelling.IndexBasedSpellChecker;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * @since solr 1.3
 */
public class SpellCheckComponentTest extends AbstractSolrTestCase {
  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(adoc("id", "0", "lowerfilt", "This is a title"));
    assertU(adoc("id", "1", "lowerfilt",
            "The quick reb fox jumped over the lazy brown dogs."));
    assertU(adoc("id", "2", "lowerfilt", "This is a document"));
    assertU(adoc("id", "3", "lowerfilt", "another document"));
    //bunch of docs that are variants on blue
    assertU(adoc("id", "4", "lowerfilt", "blue"));
    assertU(adoc("id", "5", "lowerfilt", "blud"));
    assertU(adoc("id", "6", "lowerfilt", "boue"));
    assertU(adoc("id", "7", "lowerfilt", "glue"));
    assertU(adoc("id", "8", "lowerfilt", "blee"));
    assertU("commit", commit());
  }
  
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
    assertTrue("command is null and it shouldn't be", cmdExec != null);
    assertTrue(cmdExec + " is not equal to " + "build",
            cmdExec.equals("build") == true);
    NamedList spellCheck = (NamedList) values.get("spellcheck");
    assertTrue("spellCheck is null and it shouldn't be", spellCheck != null);
    NamedList suggestions = (NamedList) spellCheck.get("suggestions");
    assertTrue("suggestions is null and it shouldn't be", suggestions != null);
    NamedList blue = (NamedList) suggestions.get("bluo");
    assertTrue(blue.get("numFound") + " is not equal to " + "5", blue
            .get("numFound").toString().equals("5") == true);
    Collection<String> theSuggestion = (Collection<String>) blue.get("suggestion");
    assertTrue("theSuggestion is null and it shouldn't be: " + blue,
            theSuggestion != null);
    assertTrue("theSuggestion Size: " + theSuggestion.size() + " is not: " + 5,
            theSuggestion.size() == 5);
    //we know there are at least 5, but now only get 3
    params.remove(SpellCheckComponent.SPELLCHECK_COUNT);
    params.remove(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS);
    params.remove(SpellCheckComponent.SPELLCHECK_BUILD);
    params.add(SpellCheckComponent.SPELLCHECK_COUNT, String.valueOf(3));
    params.add(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, String.valueOf(true));
    params.add(SpellCheckComponent.SPELLCHECK_BUILD, "false");
    rsp = new SolrQueryResponse();
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();

    spellCheck = (NamedList) values.get("spellcheck");
    assertTrue("spellCheck is null and it shouldn't be", spellCheck != null);
    suggestions = (NamedList) spellCheck.get("suggestions");
    assertTrue("suggestions is null and it shouldn't be", suggestions != null);
    blue = (NamedList) suggestions.get("bluo");
    assertTrue(blue.get("numFound") + " is not equal to " + "3", blue
            .get("numFound").toString().equals("3") == true);
    SimpleOrderedMap theSuggestions;
    int idx = blue.indexOf("suggestion", 0);
    theSuggestions = (SimpleOrderedMap) blue.get("suggestion", idx);
    assertTrue("theSuggestion is null and it shouldn't be: " + blue,
            theSuggestions != null);
    assertTrue("theSuggestions Size: " + theSuggestions.size() + " is not: " + 2,
            theSuggestions.size() == 2);//the word and the frequency

    idx = blue.indexOf("suggestion", idx + 1);
    theSuggestions = (SimpleOrderedMap) blue.get("suggestion", idx);
    assertTrue("theSuggestion is null and it shouldn't be: " + blue,
            theSuggestions != null);
    assertTrue("theSuggestions Size: " + theSuggestions.size() + " is not: " + 2,
            theSuggestions.size() == 2);//the word and the frequency

    idx = blue.indexOf("suggestion", idx + 1);
    theSuggestions = (SimpleOrderedMap) blue.get("suggestion", idx);
    assertTrue("theSuggestion is null and it shouldn't be: " + blue,
            theSuggestions != null);
    assertTrue("theSuggestions Size: " + theSuggestions.size() + " is not: " + 2,
            theSuggestions.size() == 2);//the word and the frequency

    idx = blue.indexOf("suggestion", idx + 1);
    assertTrue(idx + " does not equal: " + -1, idx == -1);
  }

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
    assertTrue("spellCheck is null and it shouldn't be", spellCheck != null);
    NamedList suggestions = (NamedList) spellCheck.get("suggestions");
    assertTrue("suggestions is null and it shouldn't be", suggestions != null);
    NamedList document = (NamedList) suggestions.get("documemt");
    assertTrue(document.get("numFound") + " is not equal to " + "1", document
            .get("numFound").toString().equals("1") == true);
    assertTrue(document.get("startOffset") + " is not equal to " + "0", document
            .get("startOffset").toString().equals("0") == true);
    assertTrue(document.get("endOffset") + " is not equal to " + "documemt".length(), document
            .get("endOffset").toString().equals(String.valueOf("documemt".length())) == true);
    Collection<String> theSuggestion = (Collection<String>) document.get("suggestion");
    assertTrue("theSuggestion is null and it shouldn't be: " + document,
            theSuggestion != null);
    assertTrue("theSuggestion Size: " + theSuggestion.size() + " is not: " + 1,
            theSuggestion.size() == 1);
    assertTrue(theSuggestion.iterator().next() + " is not equal to " + "document", theSuggestion.iterator().next().equals("document") == true);

  }


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
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();
    NamedList spellCheck = (NamedList) values.get("spellcheck");
    assertTrue("spellCheck is null and it shouldn't be", spellCheck != null);
    NamedList suggestions = (NamedList) spellCheck.get("suggestions");
    assertTrue("suggestions is null and it shouldn't be", suggestions != null);
    String collation = (String) suggestions.get("collation");
    assertTrue("collation is null and it shouldn't be", collation != null);
    assertTrue(collation + " is not equal to " + "document", collation.equals("document") == true);
    params.remove(CommonParams.Q);
    params.add(CommonParams.Q, "documemt lowerfilt:broen^4");
    handler = core.getRequestHandler("spellCheckCompRH");
    rsp = new SolrQueryResponse();
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    values = rsp.getValues();
    spellCheck = (NamedList) values.get("spellcheck");
    assertTrue("spellCheck is null and it shouldn't be", spellCheck != null);
    suggestions = (NamedList) spellCheck.get("suggestions");
    assertTrue("suggestions is null and it shouldn't be", suggestions != null);
    collation = (String) suggestions.get("collation");
    assertTrue("collation is null and it shouldn't be", collation != null);
    assertTrue(collation + " is not equal to " + "document lowerfilt:brown^4", collation.equals("document lowerfilt:brown^4") == true);

  }

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
  }

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
