package org.apache.solr.update;

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

import java.io.File;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.junit.Test;

/**
 * Testcase for {@link SolrIndexConfig}
 */
public class SolrIndexConfigTest extends SolrTestCaseJ4 {

  @Test
  public void testFailingSolrIndexConfigCreation() {
    try {
      SolrConfig solrConfig = new SolrConfig("bad-mp-solrconfig.xml");
      SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null, null);
      IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema("schema.xml", solrConfig);
      solrIndexConfig.toIndexWriterConfig(indexSchema);
      fail("a mergePolicy should have an empty constructor in order to be instantiated in Solr thus this should fail ");
    } catch (Exception e) {
      // it failed as expected
    }
  }

  @Test
  public void testTieredMPSolrIndexConfigCreation() throws Exception {
    SolrConfig solrConfig = new SolrConfig("solr" + File.separator
        + "collection1", "solrconfig-mergepolicy.xml", null);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null,
        null);
    assertNotNull(solrIndexConfig);
    assertEquals("org.apache.lucene.index.TieredMergePolicy",
        solrIndexConfig.defaultMergePolicyClassName);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema("schema.xml", solrConfig);
    solrIndexConfig.toIndexWriterConfig(indexSchema);
  }

}
