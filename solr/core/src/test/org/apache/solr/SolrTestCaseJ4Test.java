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

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class SolrTestCaseJ4Test extends SolrTestCaseJ4 {

  private static String tmpSolrHome;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Create a temporary directory that holds a core NOT named "collection1". Use the smallest configuration sets
    // we can so we don't copy that much junk around.
    tmpSolrHome = SolrTestUtil.createTempDir().toFile().getAbsolutePath();

    File subHome = new File(new File(tmpSolrHome, "core0"), "conf");
    assertTrue("Failed to make subdirectory ", subHome.mkdirs());
    String top = SolrTestUtil.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, "schema-tiny.xml"), new File(subHome, "schema-tiny.xml"));
    FileUtils.copyFile(new File(top, "solrconfig-minimal.xml"), new File(subHome, "solrconfig-minimal.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));

    FileUtils.copyDirectory(new File(tmpSolrHome, "core0"), new File(tmpSolrHome, "core1"));
    // Core discovery will default to the name of the dir the core.properties file is in. So if everything else is
    // OK as defaults, just the _presence_ of this file is sufficient.
    FileUtils.touch(new File(tmpSolrHome, "core0/core.properties"));
    FileUtils.touch(new File(tmpSolrHome, "core1/core.properties"));

    FileUtils.copyFile(SolrTestUtil.getFile("solr/solr.xml"), new File(tmpSolrHome, "solr.xml"));

    initCore("solrconfig-minimal.xml", "schema-tiny.xml", tmpSolrHome, "core1");
  }

  @After
  public void tearDown() throws Exception {
    deleteCore();
    super.tearDown();
  }

  @Test
  public void testCorrectCore() throws Exception {
    SolrCore core = h.getCore();
    assertEquals("should be core1", "core1", core.getName());
    core.close();
  }

  @Test
  public void testParams() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals(params.toString(), params().toString());

    params.add("q", "*:*");
    assertEquals(params.toString(), params("q", "*:*").toString());

    params.add("rows", "42");
    assertEquals(params.toString(), params("q", "*:*", "rows", "42").toString());

    SolrTestCaseUtil.expectThrows(RuntimeException.class, () -> {
      params("parameterWithoutValue");
    });

    SolrTestCaseUtil.expectThrows(RuntimeException.class, () -> {
      params("q", "*:*", "rows", "42", "parameterWithoutValue");
    });
  }

}
