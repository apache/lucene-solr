package org.apache.solr.uima.processor;

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

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCase for {@link UIMAUpdateRequestProcessor}
 * 
 * @version $Id$
 */
public class UIMAUpdateRequestProcessorTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", "solr-uima");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testProcessorConfiguration() {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain("uima");
    assertNotNull(chained);
    UIMAUpdateRequestProcessorFactory factory = (UIMAUpdateRequestProcessorFactory) chained
            .getFactories()[0];
    assertNotNull(factory);
    UpdateRequestProcessor processor = factory.getInstance(req(), null, null);
    assertTrue(processor instanceof UIMAUpdateRequestProcessor);
  }

  @Test
  public void testMultiMap() {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain("uima-multi-map");
    assertNotNull(chained);
    UIMAUpdateRequestProcessorFactory factory = (UIMAUpdateRequestProcessorFactory) chained
            .getFactories()[0];
    assertNotNull(factory);
    UpdateRequestProcessor processor = factory.getInstance(req(), null, null);
    assertTrue(processor instanceof UIMAUpdateRequestProcessor);
    SolrUIMAConfiguration conf = ((UIMAUpdateRequestProcessor)processor).solrUIMAConfiguration;
    Map<String, Map<String, String>> map = conf.getTypesFeaturesFieldsMapping();
    Map<String, String> subMap = map.get("a-type-which-can-have-multiple-features");
    assertEquals(2, subMap.size());
    assertEquals("1", subMap.get("A"));
    assertEquals("2", subMap.get("B"));
  }

  @Test
  public void testProcessing() throws Exception {

    addDoc(adoc(
            "id",
            "2312312321312",
            "text",
            "SpellCheckComponent got improvement related to recent Lucene changes. \n  "
                    + "Add support for specifying Spelling SuggestWord Comparator to Lucene spell "
                    + "checkers for SpellCheckComponent. Issue SOLR-2053 is already fixed, patch is"
                    + " attached if you need it, but it is also committed to trunk and 3_x branch."
                    + " Last Lucene European Conference has been held in Prague."));
    assertU(commit());
    assertQ(req("sentence:*"), "//*[@numFound='1']");
    assertQ(req("sentiment:*"), "//*[@numFound='0']");
    assertQ(req("entity:Prague"), "//*[@numFound='1']");
  }

  @Test
  public void testTwoUpdates() throws Exception {

    addDoc(adoc("id", "1", "text", "The Apache Software Foundation is happy to announce "
            + "BarCampApache Sydney, Australia, the first ASF-backed event in the Southern "
            + "Hemisphere!"));
    assertU(commit());
    assertQ(req("sentence:*"), "//*[@numFound='1']");

    addDoc(adoc("id", "2", "text", "Taking place 11th December 2010 at the University "
            + "of Sydney's Darlington Centre, the BarCampApache \"unconference\" will be"
            + " attendee-driven, facilitated by members of the Apache community and will "
            + "focus on the Apache..."));
    assertU(commit());
    assertQ(req("sentence:*"), "//*[@numFound='2']");

    assertQ(req("sentiment:positive"), "//*[@numFound='1']");
    assertQ(req("entity:Apache"), "//*[@numFound='2']");
  }

  private void addDoc(String doc) throws Exception {
    Map<String, String[]> params = new HashMap<String, String[]>();
    params.put(UpdateParams.UPDATE_CHAIN, new String[] { "uima" });
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(), (SolrParams) mmparams) {
    };

    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init(null);
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>(2);
    streams.add(new ContentStreamBase.StringStream(doc));
    req.setContentStreams(streams);
    handler.handleRequestBody(req, new SolrQueryResponse());
  }

}
