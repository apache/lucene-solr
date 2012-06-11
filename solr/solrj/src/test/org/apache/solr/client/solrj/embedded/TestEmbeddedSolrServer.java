package org.apache.solr.client.solrj.embedded;

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
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.solr.core.SolrCore;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

public class TestEmbeddedSolrServer extends AbstractEmbeddedSolrServerTestCase {

  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  protected static Logger log = LoggerFactory.getLogger(TestEmbeddedSolrServer.class);

  protected EmbeddedSolrServer getSolrCore1() {
    return new EmbeddedSolrServer(cores, "core1");
  }

  @Override
  protected File getSolrXml() throws Exception {
    return new File(SOLR_HOME, "solr.xml");
  }

  public void testGetCoreContainer() {
    Assert.assertEquals(cores, ((EmbeddedSolrServer)getSolrCore0()).getCoreContainer());
    Assert.assertEquals(cores, ((EmbeddedSolrServer)getSolrCore1()).getCoreContainer());
  }
  
  public void testShutdown() {
    
    EmbeddedSolrServer solrServer = (EmbeddedSolrServer)getSolrCore0();
    
    Assert.assertEquals(3, cores.getCores().size());
    List<SolrCore> solrCores = new ArrayList<SolrCore>();
    for (SolrCore solrCore : cores.getCores()) {
      Assert.assertEquals(false, solrCore.isClosed());
      solrCores.add(solrCore);
    }
    
    solrServer.shutdown();
    
    Assert.assertEquals(0, cores.getCores().size());
    
    for (SolrCore solrCore : solrCores) {
      Assert.assertEquals(true, solrCore.isClosed());
    }
  }

}
