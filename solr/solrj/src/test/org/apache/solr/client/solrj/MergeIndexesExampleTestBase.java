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

package org.apache.solr.client.solrj;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest.ACTION;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.ExternalPaths;

import java.io.File;
import java.io.IOException;

/**
 * Abstract base class for testing merge indexes command
 *
 * @since solr 1.4
 *
 */
public abstract class MergeIndexesExampleTestBase extends SolrExampleTestBase {
  // protected static final CoreContainer cores = new CoreContainer();
  protected static CoreContainer cores;
  private File dataDir2;

  @Override
  public String getSolrHome() {
    return ExternalPaths.EXAMPLE_MULTICORE_HOME;
  }

  @Override
  public String getSchemaFile() {
    return getSolrHome() + "/core0/conf/schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return getSolrHome() + "/core0/conf/solrconfig.xml";
  }

  public void setUp() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    super.setUp();

    cores = h.getCoreContainer();
    SolrCore.log.info("CORES=" + cores + " : " + cores.getCoreNames());
    cores.setPersistent(false);
    
    // setup datadirs
    System.setProperty( "solr.core0.data.dir", this.dataDir.getCanonicalPath() ); 
    
    dataDir2 = new File(TEMP_DIR, getClass().getName() + "-"
        + System.currentTimeMillis());
    dataDir2.mkdirs();
    
    System.setProperty( "solr.core1.data.dir", this.dataDir2.getCanonicalPath() ); 
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    
    String skip = System.getProperty("solr.test.leavedatadir");
    if (null != skip && 0 != skip.trim().length()) {
      System.err.println("NOTE: per solr.test.leavedatadir, dataDir2 will not be removed: " + dataDir2.getAbsolutePath());
    } else {
      if (!recurseDelete(dataDir2)) {
        System.err.println("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
      }
    }

  }

  @Override
  protected final SolrServer getSolrServer() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected final SolrServer createNewSolrServer() {
    throw new UnsupportedOperationException();
  }

  protected abstract SolrServer getSolrCore0();

  protected abstract SolrServer getSolrCore1();

  protected abstract SolrServer getSolrAdmin();

  protected abstract SolrServer getSolrCore(String name);

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
}
