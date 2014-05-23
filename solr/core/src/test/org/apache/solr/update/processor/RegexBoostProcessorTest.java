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
package org.apache.solr.update.processor;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RegexBoostProcessorTest extends SolrTestCaseJ4 {
  private static RegexpBoostProcessor reProcessor;
  protected static SolrRequestParsers _parser;
  protected static ModifiableSolrParams parameters;
  private static RegexpBoostProcessorFactory factory;
  private SolrInputDocument document;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
    SolrCore core = h.getCore();
    _parser = new SolrRequestParsers( null );
    SolrQueryResponse resp = null;
    parameters = new ModifiableSolrParams();
    parameters.set(RegexpBoostProcessor.BOOST_FILENAME_PARAM, "regex-boost-processor-test.txt");
    parameters.set(RegexpBoostProcessor.INPUT_FIELD_PARAM, "url");
    parameters.set(RegexpBoostProcessor.BOOST_FIELD_PARAM, "urlboost");
    SolrQueryRequest req = _parser.buildRequestFrom(core, new ModifiableSolrParams(), null);
    factory = new RegexpBoostProcessorFactory();
    factory.init(parameters.toNamedList());
    reProcessor = (RegexpBoostProcessor) factory.getInstance(req, resp, null);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // null static members for gc
    reProcessor = null;
    _parser = null;
    parameters = null;
    factory = null;
  }

  @Before
  public void setUp() throws Exception {
    document = new SolrInputDocument();
    super.setUp();
  }

  @Test
  public void testNoBoost() throws Exception {
    document.addField("id", "doc1");
    document.addField("url", "http://www.nomatch.no");
    
    processAdd(document);
    
    assertEquals(1.0d, document.getFieldValue("urlboost"));
  }
  
  @Test
  public void testDeboostOld() throws Exception {
    document.addField("id", "doc1");
    document.addField("url", "http://www.somedomain.no/old/test.html");
    
    processAdd(document);
    
    assertEquals(0.1d, document.getFieldValue("urlboost"));

    // Test the other deboost rule
    document = new SolrInputDocument();
    document.addField("id", "doc1");
    document.addField("url", "http://www.somedomain.no/foo/index(1).html");
    
    processAdd(document);
    
    assertEquals(0.5d, document.getFieldValue("urlboost"));
}
  
  @Test
  public void testBoostGood() throws Exception {
    document.addField("id", "doc1");
    document.addField("url", "http://www.mydomain.no/fifty-percent-boost");
    
    processAdd(document);
    
    assertEquals(1.5d, document.getFieldValue("urlboost"));
  }
  
  @Test
  public void testTwoRules() throws Exception {
    document.addField("id", "doc1");
    document.addField("url", "http://www.mydomain.no/old/test.html");
    
    processAdd(document);
    
    assertEquals(0.15d, document.getFieldValue("urlboost"));
  }
  
  private void processAdd(SolrInputDocument doc) throws Exception {
    AddUpdateCommand addCommand = new AddUpdateCommand(null);
    addCommand.solrDoc = doc;
    reProcessor.processAdd(addCommand);
  }
  
}
