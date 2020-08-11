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
package org.apache.solr.client.solrj;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;

/**
 * Abstract base class for testing merge indexes command
 *
 * @since solr 1.4
 *
 */
public abstract class MergeIndexesExampleTestBase extends SolrTestCaseJ4 {

  protected CoreContainer cores;
  private String saveProp;
  private File dataDir1;
  private File dataDir2;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Path getSolrHome() {
    return SolrTestCaseJ4.getFile("solrj/solr/multicore").toPath();
  }

  protected void setupCoreContainer() {
    cores = new CoreContainer(getSolrHome(), new Properties());
    cores.load();
    //cores = CoreContainer.createAndLoad(getSolrHome(), new File(TEMP_DIR, "solr.xml"));
  }
  
  @Override
  public void setUp() throws Exception {
    saveProp = System.getProperty("solr.directoryFactory");
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    super.setUp();
    File dataDir1 = createTempDir().toFile();
    // setup datadirs
    System.setProperty( "solr.core0.data.dir", dataDir1.getCanonicalPath() );

    dataDir2 = createTempDir().toFile();

    System.setProperty( "solr.core1.data.dir", this.dataDir2.getCanonicalPath() );

    setupCoreContainer();
    if (log.isInfoEnabled()) {
      log.info("CORES={} : {}", cores, cores.getLoadedCoreNames());
    }
    cores.getAllowPaths().add(dataDir1.toPath());
    cores.getAllowPaths().add(dataDir2.toPath());
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    cores.shutdown();
    
    if (saveProp == null) System.clearProperty("solr.directoryFactory");
    else System.setProperty("solr.directoryFactory", saveProp);
  }

  protected abstract SolrClient getSolrCore0();

  protected abstract SolrClient getSolrCore1();

  protected abstract SolrClient getSolrAdmin();

  protected abstract SolrClient getSolrCore(String name);

  protected abstract String getIndexDirCore1();

  private UpdateRequest setupCores() throws SolrServerException, IOException {
    UpdateRequest up = new UpdateRequest();
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    up.deleteByQuery("*:*");
    up.process(getSolrCore0());
    up.process(getSolrCore1());
    up.clear();

    // Add something to each core
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "AAA");
    doc.setField("name", "core0");

    // Add to core0
    up.add(doc);
    up.process(getSolrCore0());

    // Add to core1
    doc.setField("id", "BBB");
    doc.setField("name", "core1");
    up.add(doc);
    up.process(getSolrCore1());

    // Now Make sure AAA is in 0 and BBB in 1
    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest(q);
    q.setQuery("id:AAA");
    assertEquals(1, r.process(getSolrCore0()).getResults().size());
    assertEquals(0, r.process(getSolrCore1()).getResults().size());

    assertEquals(1,
        getSolrCore0().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(0,
        getSolrCore0().query(new SolrQuery("id:BBB")).getResults().size());

    assertEquals(0,
        getSolrCore1().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(1,
        getSolrCore1().query(new SolrQuery("id:BBB")).getResults().size());

    return up;
  }

  public void testMergeIndexesByDirName() throws Exception {
    UpdateRequest up = setupCores();

    // Now get the index directory of core1 and merge with core0
    CoreAdminRequest.mergeIndexes("core0", new String[] {getIndexDirCore1()}, new String[0], getSolrAdmin());

    // Now commit the merged index
    up.clear(); // just do commit
    up.process(getSolrCore0());

    assertEquals(1,
        getSolrCore0().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(1,
        getSolrCore0().query(new SolrQuery("id:BBB")).getResults().size());
  }

  public void testMergeIndexesByCoreName() throws Exception {
    UpdateRequest up = setupCores();
    CoreAdminRequest.mergeIndexes("core0", new String[0], new String[] {"core1"}, getSolrAdmin());

    // Now commit the merged index
    up.clear(); // just do commit
    up.process(getSolrCore0());

    assertEquals(1,
        getSolrCore0().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(1,
        getSolrCore0().query(new SolrQuery("id:BBB")).getResults().size());
  }

  public void testMergeMultipleRequest() throws Exception {
    CoreAdminRequest.MergeIndexes req = new CoreAdminRequest.MergeIndexes();
    req.setCoreName("core0");
    req.setIndexDirs(Arrays.asList("/path/1", "/path/2"));
    req.setSrcCores(Arrays.asList("core1", "core2"));
    SolrParams params = req.getParams();
    assertEquals(2, params.getParams(CoreAdminParams.SRC_CORE).length);
    assertEquals(2, params.getParams(CoreAdminParams.INDEX_DIR).length);
  }
}
