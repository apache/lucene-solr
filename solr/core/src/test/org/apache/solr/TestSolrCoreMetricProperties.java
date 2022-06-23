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
package org.apache.solr;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * <p> Test for Loading core properties from a properties file </p>
 *
 *
 * @since solr 1.4
 */
public class TestSolrCoreMetricProperties extends SolrTestCaseJ4 {

  @Override
  public void tearDown() throws Exception {
    deleteCore();
    super.tearDown();
  }

  @Test
  public void testCoreLevelMetricsEnabledDefault() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    assertEquals("solr.core.collection1", h.getCore().getSolrMetricsContext().registry);
  }

  @Test
  public void testCoreLevelMetricsEnabled() throws Exception {
    System.setProperty("coreLevelMetricsEnabled", "true");
    initCore("solrconfig.xml", "schema.xml");
    assertEquals("solr.core.collection1", h.getCore().getSolrMetricsContext().registry);
    System.clearProperty("coreLevelMetricsEnabled");
  }

  @Test
  public void testCoreLevelMetricsDisabled() throws Exception {
    System.setProperty("coreLevelMetricsEnabled", "false");
    initCore("solrconfig.xml", "schema.xml");
    assertEquals("solr.node", h.getCore().getSolrMetricsContext().registry);
    System.clearProperty("coreLevelMetricsEnabled");
  }

}
