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
package org.apache.solr.client.solrj.embedded;

import org.apache.solr.client.solrj.MergeIndexesExampleTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.core.SolrCore;

/**
 * Test for merge indexes command
 *
 * @since solr 1.4
 *
 */
public class MergeIndexesEmbeddedTest extends MergeIndexesExampleTestBase {

  @Override
  public void setUp() throws Exception {
    // TODO: fix this test to use MockDirectoryFactory
    System.clearProperty("solr.directoryFactory");
    super.setUp();
  }

  @Override
  protected SolrClient getSolrCore0() {
    return new EmbeddedSolrServer(cores, "core0");
  }

  @Override
  protected SolrClient getSolrCore1() {
    return new EmbeddedSolrServer(cores, "core1");
  }

  @Override
  protected SolrClient getSolrCore(String name) {
    return new EmbeddedSolrServer(cores, name);
  }

  @Override
  protected SolrClient getSolrAdmin() {
    return new EmbeddedSolrServer(cores, null);
  }

  @Override
  protected String getIndexDirCore1() {
    try (SolrCore core1 = cores.getCore("core1")) {
      return core1.getIndexDir();
    }
  }
}
