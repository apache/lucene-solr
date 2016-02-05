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
package org.apache.solr.update;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.Test;

public class DataDrivenBlockJoinTest extends SolrTestCaseJ4 {
  private File tmpSolrHome;
  private File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";


  @Before
  public void before() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-schemaless.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-add-schema-fields-update-processor.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig.snippet.randomindexconfig.xml"), tmpConfDir);

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig-schemaless.xml", "schema-add-schema-fields-update-processor.xml", tmpSolrHome.getPath());
  }

  @Test
  public void testAddNestedDocuments() throws Exception {
    assertU("<add>"
        + "  <doc>"
        + "    <field name='id'>1</field>"
        + "    <field name='parent'>X</field>"
        + "    <field name='hierarchical_numbering'>8</field>"
        + "    <doc>"
        + "      <field name='id'>2</field>"
        + "      <field name='child'>y</field>"
        + "      <field name='hierarchical_numbering'>8.138</field>"
        + "      <doc>" 
        + "        <field name='id'>3</field>"
        + "        <field name='grandchild'>z</field>"
        + "        <field name='hierarchical_numbering'>8.138.4498</field>"
        + "      </doc>"
        + "    </doc>"
        + "  </doc>"
        + "</add>");
    assertU("<commit/>");
  }
}

