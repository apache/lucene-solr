package org.apache.solr;

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

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;


public class SolrTestCaseJ4Test extends SolrTestCaseJ4 {

  private static String tmpSolrHome;

  @BeforeClass
  public static void beforeClass() throws Exception {

    // Create a temporary directory that holds a core NOT named "collection1". Use the smallest configuration sets
    // we can so we don't copy that much junk around.
    createTempDir();
    tmpSolrHome = TEMP_DIR + File.separator + SolrTestCaseJ4Test.class.getSimpleName() + System.currentTimeMillis();

    File subHome = new File(new File(tmpSolrHome, "core0"), "conf");
    assertTrue("Failed to make subdirectory ", subHome.mkdirs());
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, "schema-tiny.xml"), new File(subHome, "schema-tiny.xml"));
    FileUtils.copyFile(new File(top, "solrconfig-minimal.xml"), new File(subHome, "solrconfig-minimal.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));

    FileUtils.copyDirectory(new File(tmpSolrHome, "core0"), new File(tmpSolrHome, "core1"));

    FileUtils.copyFile(getFile("solr/solr-multicore.xml"), new File(tmpSolrHome, "solr.xml"));

    initCore("solrconfig-minimal.xml", "schema-tiny.xml", tmpSolrHome, "core1");
  }

  @AfterClass
  public static void AfterClass() throws Exception {
    FileUtils.deleteDirectory(new File(tmpSolrHome).getAbsoluteFile());
  }

  @Test
  public void testCorrectCore() throws Exception {
    assertEquals("should be core1", "core1", h.getCore().getName());
  }
}
