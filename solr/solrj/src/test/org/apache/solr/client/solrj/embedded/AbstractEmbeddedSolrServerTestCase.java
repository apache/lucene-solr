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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public abstract class AbstractEmbeddedSolrServerTestCase extends LuceneTestCase {

  protected static Logger log = LoggerFactory.getLogger(AbstractEmbeddedSolrServerTestCase.class);

  protected static final File SOLR_HOME = SolrTestCaseJ4.getFile("solrj/solr/shared");

  protected CoreContainer cores = null;
  protected File tempDir;

  protected void createTempDir() {
    if (tempDir == null) {
      tempDir = new File(TEMP_DIR, "solrtest-" + getTestClass().getSimpleName() + "-" + System.currentTimeMillis());
      tempDir.mkdirs();
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    System.setProperty("solr.solr.home", SOLR_HOME.getAbsolutePath());
    System.out.println("Solr home: " + SOLR_HOME.getAbsolutePath());

    //The index is always stored within a temporary directory
    createTempDir();
    
    File dataDir = new File(tempDir,"data1");
    File dataDir2 = new File(tempDir,"data2");
    System.setProperty("dataDir1", dataDir.getAbsolutePath());
    System.setProperty("dataDir2", dataDir2.getAbsolutePath());
    System.setProperty("tempDir", tempDir.getAbsolutePath());
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    cores = CoreContainer.createAndLoad(SOLR_HOME.getAbsolutePath(), getSolrXml());
    //cores.setPersistent(false);
  }
  
  protected abstract File getSolrXml() throws Exception;

  @Override
  @After
  public void tearDown() throws Exception {
    if (cores != null)
      cores.shutdown();

    System.clearProperty("dataDir1");
    System.clearProperty("dataDir2");
    System.clearProperty("tests.shardhandler.randomSeed");

    deleteAdditionalFiles();

    File dataDir = new File(tempDir,"data");
    String skip = System.getProperty("solr.test.leavedatadir");
    if (null != skip && 0 != skip.trim().length()) {
      log.info("NOTE: per solr.test.leavedatadir, dataDir will not be removed: " + dataDir.getAbsolutePath());
    } else {
      //Removing the temporary directory which contains the index (all other files should have been removed before)
      if (!AbstractSolrTestCase.recurseDelete(tempDir)) {
        log.warn("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
      }
    }

    super.tearDown();
  }

  protected void deleteAdditionalFiles() {

  }

  protected SolrServer getSolrCore0() {
    return getSolrCore("core0");
  }

  protected SolrServer getSolrCore1() {
    return getSolrCore("core1");
  }

  protected SolrServer getSolrCore(String name) {
    return new EmbeddedSolrServer(cores, name);
  }

}
