package org.apache.solr.client.solrj.embedded;

/**
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SystemPropertiesRestoreRule;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.FileUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEmbeddedSolrServer extends LuceneTestCase {

  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  protected static Logger log = LoggerFactory.getLogger(TestEmbeddedSolrServer.class);
  
  protected CoreContainer cores = null;
  private File home;
  
  public String getSolrHome() {
    return "solrj/solr/shared";
  }

  public String getOrigSolrXml() {
    return "solr.xml";
  }

  public String getSolrXml() {
    return "test-solr.xml";
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.solr.home", getSolrHome());
    
    home = SolrTestCaseJ4.getFile(getSolrHome());
    System.setProperty("solr.solr.home", home.getAbsolutePath());

    log.info("pwd: " + (new File(".")).getAbsolutePath());
    File origSolrXml = new File(home, getOrigSolrXml());
    File solrXml = new File(home, getSolrXml());
    FileUtils.copyFile(origSolrXml, solrXml);
    cores = new CoreContainer(home.getAbsolutePath(), solrXml);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (cores != null) {
      cores.shutdown();
    }
    File dataDir = new File(home,"data");
    if (!AbstractSolrTestCase.recurseDelete(dataDir)) {
      log.warn("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
    }
    super.tearDown();
  }
  
  protected EmbeddedSolrServer getSolrCore0() {
    return new EmbeddedSolrServer(cores, "core0");
  }

  protected EmbeddedSolrServer getSolrCore1() {
    return new EmbeddedSolrServer(cores, "core1");
  }
  
  public void testGetCoreContainer() {
    Assert.assertEquals(cores, getSolrCore0().getCoreContainer());
    Assert.assertEquals(cores, getSolrCore1().getCoreContainer());
  }
  
  public void testShutdown() {
    
    EmbeddedSolrServer solrServer = getSolrCore0();
    
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
