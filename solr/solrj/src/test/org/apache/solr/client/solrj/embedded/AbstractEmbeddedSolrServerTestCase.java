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
import java.nio.file.Path;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEmbeddedSolrServerTestCase extends SolrTestCaseJ4 {

  protected static Logger log = LoggerFactory.getLogger(AbstractEmbeddedSolrServerTestCase.class);

  protected static final Path SOLR_HOME = getFile("solrj/solr/shared").toPath().toAbsolutePath();

  protected CoreContainer cores = null;
  protected File tempDir;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    System.setProperty("solr.solr.home", SOLR_HOME.toString());
    System.setProperty("configSetBase", SOLR_HOME.resolve("../configsets").normalize().toString());
    System.out.println("Solr home: " + SOLR_HOME.toString());

    //The index is always stored within a temporary directory
    tempDir = createTempDir().toFile();
    
    File dataDir = new File(tempDir,"data1");
    File dataDir2 = new File(tempDir,"data2");
    System.setProperty("dataDir1", dataDir.getAbsolutePath());
    System.setProperty("dataDir2", dataDir2.getAbsolutePath());
    System.setProperty("tempDir", tempDir.getAbsolutePath());
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    cores = CoreContainer.createAndLoad(SOLR_HOME, getSolrXml());
    //cores.setPersistent(false);
  }
  
  protected Path getSolrXml() throws Exception {
    return SOLR_HOME.resolve("solr.xml");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (cores != null)
      cores.shutdown();

    System.clearProperty("dataDir1");
    System.clearProperty("dataDir2");
    System.clearProperty("tests.shardhandler.randomSeed");

    deleteAdditionalFiles();

    super.tearDown();
  }

  protected void deleteAdditionalFiles() {

  }

  protected SolrClient getSolrCore0() {
    return getSolrCore("core0");
  }

  protected SolrClient getSolrCore1() {
    return getSolrCore("core1");
  }

  protected SolrClient getSolrCore(String name) {
    return new EmbeddedSolrServer(cores, name);
  }

}
