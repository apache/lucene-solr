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

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.junit.Test;

public class SolrCoreConfigTest extends AbstractFullDistribZkTestBase {
  private static final String COLLECTION1 = "collection1";

  @Override
  public void distribSetUp() throws Exception {
    System.setProperty(SolrCore.DISABLE_ZK_CONFIG_WATCH, "true");
    super.distribSetUp();
  }

  @Override
  public void distribTearDown() throws Exception {
    try {
      super.distribTearDown();
    } finally {
      System.clearProperty(SolrCore.DISABLE_ZK_CONFIG_WATCH);
    }
  }

  @Test
  public void testNoZkConfigWatch() {
    CoreContainer cc = getContainer();
    assertFalse("There shouldn't be any conf listener", cc.getZkController().hasConfDirectoryListeners("/configs/conf1"));
  }

  private CoreContainer getContainer() {
    CoreContainer queryAggregatorContainer = null;
    for (JettySolrRunner jetty : jettys) {
      return jetty.getCoreContainer();
    }
    return null;
  }
}
