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
package org.apache.solr.uima.processor;

import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.uima.processor.SolrUIMAConfiguration.MapField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCase for {@link UIMAUpdateRequestProcessor}
 * 
 *
 */
@Slow
public class UIMAUpdateRequestProcessorTest extends SolrTestCaseJ4 {

  public static final String UIMA_CHAIN = "uima";
  public static final String UIMA_MULTI_MAP_CHAIN = "uima-multi-map";
  public static final String UIMA_IGNORE_ERRORS_CHAIN = "uima-ignoreErrors";
  public static final String UIMA_NOT_IGNORE_ERRORS_CHAIN = "uima-not-ignoreErrors";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", getFile("uima/solr").getAbsolutePath());
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
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain(UIMA_CHAIN);
    assertNotNull(chained);
    UIMAUpdateRequestProcessorFactory factory = (UIMAUpdateRequestProcessorFactory)chained.getProcessors().get(0);
    assertNotNull(factory);
    UpdateRequestProcessor processor = factory.getInstance(req(), null, null);
    assertTrue(processor instanceof UIMAUpdateRequestProcessor);
  }

  @Test
  public void testMultiMap() {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain(UIMA_MULTI_MAP_CHAIN);
    assertNotNull(chained);
    UIMAUpdateRequestProcessorFactory factory = (UIMAUpdateRequestProcessorFactory)chained.getProcessors().get(0);
    assertNotNull(factory);
    UpdateRequestProcessor processor = factory.getInstance(req(), null, null);
    assertTrue(processor instanceof UIMAUpdateRequestProcessor);
    SolrUIMAConfiguration conf = ((UIMAUpdateRequestProcessor)processor).getConfiguration();
    Map<String, Map<String, MapField>> map = conf.getTypesFeaturesFieldsMapping();
    Map<String, MapField> subMap = map.get("a-type-which-can-have-multiple-features");
    assertEquals(2, subMap.size());
    assertEquals("1", subMap.get("A").getFieldName(null));
    assertEquals("2", subMap.get("B").getFieldName(null));
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
                + " Last Lucene European Conference has been held in Prague."), UIMA_CHAIN);
    assertU(commit());
    assertQ(req("sentence:*"), "//*[@numFound='1']");
    assertQ(req("sentiment:*"), "//*[@numFound='0']");
    assertQ(req("OTHER_sm:Prague"), "//*[@numFound='1']");
  }

  @Test
  public void testTwoUpdates() throws Exception {

    addDoc(adoc("id", "1", "text", "The Apache Software Foundation is happy to announce "
            + "BarCampApache Sydney, Australia, the first ASF-backed event in the Southern "
        + "Hemisphere!"), UIMA_CHAIN);
    assertU(commit());
    assertQ(req("sentence:*"), "//*[@numFound='1']");

    addDoc(adoc("id", "2", "text", "Taking place 11th December 2010 at the University "
            + "of Sydney's Darlington Centre, the BarCampApache \"unconference\" will be"
            + " attendee-driven, facilitated by members of the Apache community and will "
        + "focus on the Apache..."), UIMA_CHAIN);
    assertU(commit());
    assertQ(req("sentence:*"), "//*[@numFound='2']");

    assertQ(req("sentiment:positive"), "//*[@numFound='1']");
    assertQ(req("ORGANIZATION_sm:Apache"), "//*[@numFound='2']");
  }

  @Test
  public void testErrorHandling() throws Exception {

    try{
      addDoc(adoc(
            "id",
            "2312312321312",
            "text",
            "SpellCheckComponent got improvement related to recent Lucene changes. \n  "
                    + "Add support for specifying Spelling SuggestWord Comparator to Lucene spell "
                    + "checkers for SpellCheckComponent. Issue SOLR-2053 is already fixed, patch is"
                    + " attached if you need it, but it is also committed to trunk and 3_x branch."
                + " Last Lucene European Conference has been held in Prague."), UIMA_NOT_IGNORE_ERRORS_CHAIN);
      fail("exception shouldn't be ignored");
    }
    catch(RuntimeException expected){}
    assertU(commit());
    assertQ(req("*:*"), "//*[@numFound='0']");

    addDoc(adoc(
            "id",
            "2312312321312",
            "text",
            "SpellCheckComponent got improvement related to recent Lucene changes. \n  "
                    + "Add support for specifying Spelling SuggestWord Comparator to Lucene spell "
                    + "checkers for SpellCheckComponent. Issue SOLR-2053 is already fixed, patch is"
                    + " attached if you need it, but it is also committed to trunk and 3_x branch."
                + " Last Lucene European Conference has been held in Prague."), UIMA_IGNORE_ERRORS_CHAIN);
    assertU(commit());
    assertQ(req("*:*"), "//*[@numFound='1']");

    try{
      addDoc(adoc(
            "id",
            "2312312321312",
            "text",
          "SpellCheckComponent got improvement related to recent Lucene changes."), UIMA_NOT_IGNORE_ERRORS_CHAIN);
      fail("exception shouldn't be ignored");
    }
    catch(StringIndexOutOfBoundsException e){  // SOLR-2579
      fail("exception shouldn't be raised");
    }
    catch(SolrException expected){}

    try{
      addDoc(adoc(
            "id",
            "2312312321312",
            "text",
          "SpellCheckComponent got improvement related to recent Lucene changes."), UIMA_IGNORE_ERRORS_CHAIN);
    }
    catch(StringIndexOutOfBoundsException e){  // SOLR-2579
      fail("exception shouldn't be raised");
    }
  }

  @Test
  public void testMultiplierProcessing() throws Exception {
    for (int i = 0; i < RANDOM_MULTIPLIER; i++) {
      testProcessing();
    }
  }

}
