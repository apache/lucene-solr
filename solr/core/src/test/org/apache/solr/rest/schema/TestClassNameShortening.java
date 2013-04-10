package org.apache.solr.rest.schema;
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

import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.restlet.ext.servlet.ServerServlet;

import java.util.SortedMap;
import java.util.TreeMap;


public class TestClassNameShortening extends RestTestBase {

  @BeforeClass
  public static void init() throws Exception {
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<ServletHolder,String>();
    final ServletHolder solrRestApi = new ServletHolder("SolrRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrRestApi");
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'

    createJettyAndHarness(TEST_HOME(), "solrconfig-minimal.xml", "schema-class-name-shortening-on-serialization.xml", 
                          "/solr", true, extraServlets);
  }

  @Test
  public void testClassNamesNotShortened() throws Exception {
    assertQ("/schema/fieldtypes/fullClassNames?indent=on&wt=xml&showDefaults=true",
            "count(/response/lst[@name='fieldType']) = 1",
            "/response/lst[@name='fieldType']/str[@name='class'] = 'org.apache.solr.schema.TextField'",
            "//arr[@name='charFilters']/lst/str[@name='class'] = 'org.apache.solr.analysis.MockCharFilterFactory'",
            "//lst[@name='tokenizer']/str[@name='class'] = 'org.apache.solr.analysis.MockTokenizerFactory'",
            "//arr[@name='filters']/lst/str[@name='class'] = 'org.apache.solr.analysis.MockTokenFilterFactory'",
            "/response/lst[@name='fieldType']/lst[@name='similarity']/str[@name='class'] = 'org.apache.lucene.misc.SweetSpotSimilarity'");
  }

  @Test
  public void testShortenedGlobalSimilarityStaysShortened() throws Exception {
    assertQ("/schema/similarity?indent=on&wt=xml",
            "count(/response/lst[@name='similarity']) = 1",
            "/response/lst[@name='similarity']/str[@name='class'][.='solr.SchemaSimilarityFactory']");
  }

  @Test
  public void testShortenedClassNamesStayShortened() throws Exception {
    assertQ("/schema/fieldtypes/shortenedClassNames?indent=on&wt=xml&showDefaults=true",
            "count(/response/lst[@name='fieldType']) = 1",
            "/response/lst[@name='fieldType']/str[@name='class'] = 'solr.TextField'",
            "//arr[@name='charFilters']/lst/str[@name='class'] = 'solr.MockCharFilterFactory'",
            "//lst[@name='tokenizer']/str[@name='class'] = 'solr.MockTokenizerFactory'",
            "//arr[@name='filters']/lst/str[@name='class'] = 'solr.MockTokenFilterFactory'",
            "/response/lst[@name='fieldType']/lst[@name='similarity']/str[@name='class'] = 'solr.SweetSpotSimilarityFactory'");
  }
}
