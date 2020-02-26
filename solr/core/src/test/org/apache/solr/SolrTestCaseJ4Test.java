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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;


public class SolrTestCaseJ4Test extends SolrTestCaseJ4 {

  /**
   * Randomly test that it is possible to use SolrTestCaseJ4 even if
   * {@link org.apache.solr.util.ExternalPaths#SOURCE_HOME} is null. This
   * will be common for usages of Solr Test Framework outside of solr.
   */
  @ClassRule
  public static final SourceHomeNullifier shn = new SourceHomeNullifier();

  private static String tmpSolrHome;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Create a temporary directory that holds a core NOT named "collection1". Use the smallest configuration sets
    // we can so we don't copy that much junk around.
    tmpSolrHome = createTempDir().toFile().getAbsolutePath();

    File subHome = new File(new File(tmpSolrHome, "core0"), "conf");
    assertTrue("Failed to make subdirectory ", subHome.mkdirs());
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, "schema-tiny.xml"), new File(subHome, "schema-tiny.xml"));
    FileUtils.copyFile(new File(top, "solrconfig-minimal.xml"), new File(subHome, "solrconfig-minimal.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));

    FileUtils.copyDirectory(new File(tmpSolrHome, "core0"), new File(tmpSolrHome, "core1"));
    // Core discovery will default to the name of the dir the core.properties file is in. So if everything else is
    // OK as defaults, just the _presence_ of this file is sufficient.
    FileUtils.touch(new File(tmpSolrHome, "core0/core.properties"));
    FileUtils.touch(new File(tmpSolrHome, "core1/core.properties"));

    FileUtils.copyFile(getFile("solr/solr.xml"), new File(tmpSolrHome, "solr.xml"));

    initCore("solrconfig-minimal.xml", "schema-tiny.xml", tmpSolrHome, "core1");
  }

  @AfterClass
  public static void AfterClass() throws Exception {

  }

  @Test
  public void testCorrectCore() throws Exception {
    assertEquals("should be core1", "core1", h.getCore().getName());
  }

  @Test
  public void testParams() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals(params.toString(), params().toString());

    params.add("q", "*:*");
    assertEquals(params.toString(), params("q", "*:*").toString());

    params.add("rows", "42");
    assertEquals(params.toString(), params("q", "*:*", "rows", "42").toString());

    expectThrows(RuntimeException.class, () -> {
      params("parameterWithoutValue");
    });

    expectThrows(RuntimeException.class, () -> {
      params("q", "*:*", "rows", "42", "parameterWithoutValue");
    });
  }

}
