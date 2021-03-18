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
package org.apache.solr.rest;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Base class for Solr Rest-oriented API tests. Creates jetty and test harness
 * with solrconfig.xml and schema-rest.xml.
 *
 * Use RestTestBase instead if you need to specialize the solrconfig,
 * the schema, or jetty/test harness creation; otherwise you'll get
 * imbalanced SolrIndexSearcher closes/opens and a suite-level failure
 * for a zombie thread.
 */
abstract public class SolrRestletTestBase extends RestTestBase {

  /**
   * Creates test harness, including "extra" servlets for all
   * Solr Restlet Application subclasses.
   */
  @Before
  public void setUp() throws Exception {

    SolrTestCaseJ4.randomizeNumericTypesProperties();

    Path tempDir = SolrTestUtil.createTempDir();
    Path coresDir = tempDir.resolve("cores");

    System.setProperty("coreRootDirectory", coresDir.toString());
    System.setProperty("configSetBaseDir", SolrTestUtil.TEST_HOME());

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();

    Properties props = new Properties();
    props.setProperty("name", DEFAULT_TEST_CORENAME);
    props.setProperty("config", "solrconfig.xml");
    props.setProperty("schema", "schema-rest.xml");
    props.setProperty("configSet", "collection1");

    writeCoreProperties(coresDir.resolve("core"), props, "SolrRestletTestBase");
    jetty = createJettyAndHarness(SolrTestUtil.TEST_HOME(), "solrconfig.xml", "schema-rest.xml", "/solr", true, extraServlets);

    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    if (jetty != null) jetty.stop();
    jetty = null;
    super.tearDown();
  }
}
