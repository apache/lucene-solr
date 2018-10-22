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
package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;

/** Inspired by SOLR-4858 */
public class TestReloadAndDeleteDocs extends SolrTestCaseJ4 {
  
  @After
  public void after() throws Exception {
    System.clearProperty("enable.update.log");
    deleteCore();
  }

  public void testReloadAndDeleteDocsNoUpdateLog() throws Exception {
    doTest(false);
  }

  public void testReloadAndDeleteDocsWithUpdateLog() throws Exception {
    doTest(true);
  }

  private void doTest(final boolean useUpdateLog) throws Exception {
    System.setProperty("enable.update.log", useUpdateLog ? "true" : "false");
    initCore("solrconfig.xml", "schema.xml", TEST_HOME());
    assertEquals("UpdateLog existence doesn't match sys prop (test config changed?)",
                 useUpdateLog,
                 null != h.getCore().getUpdateHandler().getUpdateLog());
    h.reload();
    assertU("<delete><query>*:*</query></delete>");
  }
}
