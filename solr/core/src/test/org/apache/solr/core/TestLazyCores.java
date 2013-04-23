package org.apache.solr.core;

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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.RefCounted;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class TestLazyCores extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-tiny.xml");
  }

  private final File solrHomeDirectory = new File(TEMP_DIR, "org.apache.solr.core.TestLazyCores_testlazy");


  private CoreContainer init() throws Exception {

    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    for (int idx = 1; idx < 10; ++idx) {
      copyMinConf(new File(solrHomeDirectory, "collection" + idx));
    }

    File solrXml = new File(solrHomeDirectory, "solr.xml");
    FileUtils.write(solrXml, LOTS_SOLR_XML, IOUtils.CHARSET_UTF_8.toString());
    final CoreContainer cores = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    cores.load(solrHomeDirectory.getAbsolutePath(), solrXml);
    //  h.getCoreContainer().load(solrHomeDirectory.getAbsolutePath(), new File(solrHomeDirectory, "solr.xml"));

    cores.setPersistent(false);
    return cores;
  }

  @After
  public void after() throws Exception {
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
  }
  @Test
  public void testLazyLoad() throws Exception {
    CoreContainer cc = init();
    try {

      // NOTE: This checks the initial state for loading, no need to do this elsewhere.
      checkInCores(cc, "collection1", "collectionLazy2", "collectionLazy5");
      checkNotInCores(cc, "collectionLazy3", "collectionLazy4", "collectionLazy6", "collectionLazy7",
          "collectionLazy8", "collectionLazy9");

      SolrCore core1 = cc.getCore("collection1");
      assertFalse("core1 should not be transient", core1.getCoreDescriptor().isTransient());
      assertTrue("core1 should  be loadable", core1.getCoreDescriptor().isLoadOnStartup());
      assertNotNull(core1.getSolrConfig());

      SolrCore core2 = cc.getCore("collectionLazy2");
      assertTrue("core2 should not be transient", core2.getCoreDescriptor().isTransient());
      assertTrue("core2 should be loadable", core2.getCoreDescriptor().isLoadOnStartup());

      SolrCore core3 = cc.getCore("collectionLazy3");
      assertTrue("core3 should not be transient", core3.getCoreDescriptor().isTransient());
      assertFalse("core3 should not be loadable", core3.getCoreDescriptor().isLoadOnStartup());

      SolrCore core4 = cc.getCore("collectionLazy4");
      assertFalse("core4 should not be transient", core4.getCoreDescriptor().isTransient());
      assertFalse("core4 should not be loadable", core4.getCoreDescriptor().isLoadOnStartup());

      SolrCore core5 = cc.getCore("collectionLazy5");
      assertFalse("core5 should not be transient", core5.getCoreDescriptor().isTransient());
      assertTrue("core5 should  be loadable", core5.getCoreDescriptor().isLoadOnStartup());

      core1.close();
      core2.close();
      core3.close();
      core4.close();
      core5.close();
    } finally {
      cc.shutdown();
    }
  }

  // This is a little weak. I'm not sure how to test that lazy core2 is loaded automagically. The getCore
  // will, of course, load it.

  @Test
  public void testLazySearch() throws Exception {
    CoreContainer cc = init();
    try {
      // Make sure Lazy4 isn't loaded. Should be loaded on the get
      checkNotInCores(cc, "collectionLazy4");
      SolrCore core4 = cc.getCore("collectionLazy4");

      addLazy(core4, "id", "0");
      addLazy(core4, "id", "1", "v_t", "Hello Dude");
      addLazy(core4, "id", "2", "v_t", "Hello Yonik");
      addLazy(core4, "id", "3", "v_s", "{!literal}");
      addLazy(core4, "id", "4", "v_s", "other stuff");
      addLazy(core4, "id", "5", "v_f", "3.14159");
      addLazy(core4, "id", "6", "v_f", "8983");

      SolrQueryRequest req = makeReq(core4);
      CommitUpdateCommand cmtCmd = new CommitUpdateCommand(req, false);
      core4.getUpdateHandler().commit(cmtCmd);

      RefCounted<SolrIndexSearcher> holder = core4.getSearcher();
      SolrIndexSearcher searcher = holder.get();

      // Just get a couple of searches to work!
      assertQ("test prefix query",
          makeReq(core4, "q", "{!prefix f=v_t}hel", "wt", "xml")
          , "//result[@numFound='2']"
      );

      assertQ("test raw query",
          makeReq(core4, "q", "{!raw f=v_t}hello", "wt", "xml")
          , "//result[@numFound='2']"
      );

      // Now just insure that the normal searching on "collection1" finds _0_ on the same query that found _2_ above.
      // Use of makeReq above and req below is tricky, very tricky.
      assertQ("test raw query",
          req("q", "{!raw f=v_t}hello", "wt", "xml")
          , "//result[@numFound='0']"
      );

      // no analysis is done, so these should match nothing
      assertQ("test raw query",
          makeReq(core4, "q", "{!raw f=v_t}Hello", "wt", "xml")
          , "//result[@numFound='0']"
      );
      assertQ("test raw query",
          makeReq(core4, "q", "{!raw f=v_f}1.5", "wt", "xml")
          , "//result[@numFound='0']"
      );

      checkInCores(cc, "collectionLazy4");

      searcher.close();
      core4.close();
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testCachingLimit() throws Exception {
    CoreContainer cc = init();
    try {
      // First check that all the cores that should be loaded at startup actually are.

      checkInCores(cc, "collection1", "collectionLazy2", "collectionLazy5");
      checkNotInCores(cc, "collectionLazy3", "collectionLazy4", "collectionLazy6",
          "collectionLazy7", "collectionLazy8", "collectionLazy9");

      // By putting these in non-alpha order, we're also checking that we're  not just seeing an artifact.
      SolrCore core1 = cc.getCore("collection1");
      SolrCore core3 = cc.getCore("collectionLazy3");
      SolrCore core4 = cc.getCore("collectionLazy4");
      SolrCore core2 = cc.getCore("collectionLazy2");
      SolrCore core5 = cc.getCore("collectionLazy5");

      checkInCores(cc, "collection1", "collectionLazy2", "collectionLazy3", "collectionLazy4", "collectionLazy5");
      checkNotInCores(cc, "collectionLazy6", "collectionLazy7", "collectionLazy8", "collectionLazy9");

      // map should be full up, add one more and verify
      SolrCore core6 = cc.getCore("collectionLazy6");
      checkInCores(cc, "collection1", "collectionLazy2", "collectionLazy3", "collectionLazy4", "collectionLazy5",
          "collectionLazy6");
      checkNotInCores(cc, "collectionLazy7", "collectionLazy8", "collectionLazy9");

      SolrCore core7 = cc.getCore("collectionLazy7");
      checkInCores(cc, "collection1", "collectionLazy2", "collectionLazy3", "collectionLazy4", "collectionLazy5",
          "collectionLazy6", "collectionLazy7");
      checkNotInCores(cc, "collectionLazy8", "collectionLazy9");

      SolrCore core8 = cc.getCore("collectionLazy8");
      checkInCores(cc, "collection1", "collectionLazy2", "collectionLazy4", "collectionLazy5", "collectionLazy6",
          "collectionLazy7", "collectionLazy8");
      checkNotInCores(cc, "collectionLazy3", "collectionLazy9");

      SolrCore core9 = cc.getCore("collectionLazy9");
      checkInCores(cc, "collection1", "collectionLazy4", "collectionLazy5", "collectionLazy6", "collectionLazy7",
          "collectionLazy8", "collectionLazy9");
      checkNotInCores(cc, "collectionLazy2", "collectionLazy3");


      // Note decrementing the count when the core is removed from the lazyCores list is appropriate, since the
      // refcount is 1 when constructed. anyone _else_ who's opened up one has to close it.
      core1.close();
      core2.close();
      core3.close();
      core4.close();
      core5.close();
      core6.close();
      core7.close();
      core8.close();
      core9.close();
    } finally {
      cc.shutdown();
    }
  }

  // Test case for SOLR-4300

  @Test
  public void testRace() throws Exception {
    final List<SolrCore> theCores = new ArrayList<SolrCore>();
    final CoreContainer cc = init();
    try {

      Thread[] threads = new Thread[15];
      for (int idx = 0; idx < threads.length; idx++) {
        threads[idx] = new Thread() {
          @Override
          public void run() {
            SolrCore core = cc.getCore("collectionLazy3");
            synchronized (theCores) {
              theCores.add(core);
            }
          }
        };
        threads[idx].start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      for (int idx = 0; idx < theCores.size() - 1; ++idx) {
        assertEquals("Cores should be the same!", theCores.get(idx), theCores.get(idx + 1));
      }
      for (SolrCore core : theCores) {
        core.close();
      }

    } finally {
      cc.shutdown();
    }
  }

  private void tryCreateFail(CoreAdminHandler admin, String name, String dataDir, String... errs) throws Exception {
    try {
      SolrQueryResponse resp = new SolrQueryResponse();

      SolrQueryRequest request = req(CoreAdminParams.ACTION,
          CoreAdminParams.CoreAdminAction.CREATE.toString(),
          CoreAdminParams.DATA_DIR, dataDir,
          CoreAdminParams.NAME, name,
          "schema", "schema-tiny.xml",
          "config", "solrconfig-minimal.xml");

      admin.handleRequestBody(request, resp);
      fail("Should have thrown an error");
    } catch (SolrException se) {
      SolrException cause = (SolrException)se.getCause();
      assertEquals("Exception code should be 500", 500, cause.code());
      for (String err : errs) {
       assertTrue("Should have seen an exception containing the an error",
            cause.getMessage().contains(err));
      }
    }
  }
  @Test
  public void testCreateSame() throws Exception {
    final CoreContainer cc = init();
    try {
      // First, try all 4 combinations of load on startup and transient
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      SolrCore lc2 = cc.getCore("collectionLazy2");
      SolrCore lc4 = cc.getCore("collectionLazy4");
      SolrCore lc5 = cc.getCore("collectionLazy5");
      SolrCore lc6 = cc.getCore("collectionLazy6");

      copyMinConf(new File(solrHomeDirectory, "t2"));
      copyMinConf(new File(solrHomeDirectory, "t4"));
      copyMinConf(new File(solrHomeDirectory, "t5"));
      copyMinConf(new File(solrHomeDirectory, "t6"));


      // Should also fail with the same name
      tryCreateFail(admin, "collectionLazy2", "t12", "Core with name", "collectionLazy2", "already exists");
      tryCreateFail(admin, "collectionLazy4", "t14", "Core with name", "collectionLazy4", "already exists");
      tryCreateFail(admin, "collectionLazy5", "t15", "Core with name", "collectionLazy5", "already exists");
      tryCreateFail(admin, "collectionLazy6", "t16", "Core with name", "collectionLazy6", "already exists");

      lc2.close();
      lc4.close();
      lc5.close();
      lc6.close();

    } finally {
      cc.shutdown();
    }
  }

  //Make sure persisting not-loaded lazy cores is done. See SOLR-4347

  @Test
  public void testPersistence() throws Exception {
    final CoreContainer cc = init();
    try {
      copyMinConf(new File(solrHomeDirectory, "core1"));
      copyMinConf(new File(solrHomeDirectory, "core2"));
      copyMinConf(new File(solrHomeDirectory, "core3"));
      copyMinConf(new File(solrHomeDirectory, "core4"));

      cc.setPersistent(true);
      CoreDescriptor d1 = new CoreDescriptor(cc, "core1", "./core1");
      d1.setTransient(true);
      d1.setLoadOnStartup(true);
      d1.setSchemaName("schema-tiny.xml");
      d1.setConfigName("solrconfig-minimal.xml");
      SolrCore core1 = cc.create(d1);

      CoreDescriptor d2 = new CoreDescriptor(cc, "core2", "./core2");
      d2.setTransient(true);
      d2.setLoadOnStartup(false);
      d2.setSchemaName("schema-tiny.xml");
      d2.setConfigName("solrconfig-minimal.xml");
      SolrCore core2 = cc.create(d2);

      CoreDescriptor d3 = new CoreDescriptor(cc, "core3", "./core3");
      d3.setTransient(false);
      d3.setLoadOnStartup(true);
      d3.setSchemaName("schema-tiny.xml");
      d3.setConfigName("solrconfig-minimal.xml");
      SolrCore core3 = cc.create(d3);

      CoreDescriptor d4 = new CoreDescriptor(cc, "core4", "./core4");
      d4.setTransient(false);
      d4.setLoadOnStartup(false);
      d4.setSchemaName("schema-tiny.xml");
      d4.setConfigName("solrconfig-minimal.xml");
      SolrCore core4 = cc.create(d4);

      final File oneXml = new File(solrHomeDirectory, "lazy1.solr.xml");
      cc.persistFile(oneXml);

      assertXmlFile(oneXml,
          "/solr/cores/core[@name='collection1']",
          "/solr/cores/core[@name='collectionLazy2']",
          "/solr/cores/core[@name='collectionLazy3']",
          "/solr/cores/core[@name='collectionLazy4']",
          "/solr/cores/core[@name='collectionLazy5']",
          "/solr/cores/core[@name='collectionLazy6']",
          "/solr/cores/core[@name='collectionLazy7']",
          "/solr/cores/core[@name='collectionLazy8']",
          "/solr/cores/core[@name='collectionLazy9']",
          "/solr/cores/core[@name='core1']",
          "/solr/cores/core[@name='core2']",
          "/solr/cores/core[@name='core3']",
          "/solr/cores/core[@name='core4']");
      assertXmlFile(oneXml, "13=count(/solr/cores/core)");

      removeOne(cc, "collectionLazy2");
      removeOne(cc, "collectionLazy3");
      removeOne(cc, "collectionLazy4");
      removeOne(cc, "collectionLazy5");
      removeOne(cc, "collectionLazy6");
      removeOne(cc, "collectionLazy7");
      removeOne(cc, "core1");
      removeOne(cc, "core2");
      removeOne(cc, "core3");
      removeOne(cc, "core4");

      // now test that unloading a core means the core is not persisted

      final File twoXml = new File(solrHomeDirectory, "lazy2.solr.xml");
      cc.persistFile(twoXml);

      assertXmlFile(twoXml, "3=count(/solr/cores/core)");
    } finally {
      cc.shutdown();
    }
  }

  private void removeOne(CoreContainer cc, String coreName) {
    SolrCore tmp = cc.remove(coreName);
    if (tmp != null) tmp.close();
  }
  public static void checkNotInCores(CoreContainer cc, String... nameCheck) {
    Collection<String> names = cc.getCoreNames();
    for (String name : nameCheck) {
      assertFalse("core " + name + " was found in the list of cores", names.contains(name));
    }
  }

  public static void checkInCores(CoreContainer cc, String... nameCheck) {
    Collection<String> names = cc.getCoreNames();
    for (String name : nameCheck) {
      assertTrue("core " + name + " was not found in the list of cores", names.contains(name));
    }
  }

  private void addLazy(SolrCore core, String... fieldValues) throws IOException {
    UpdateHandler updater = core.getUpdateHandler();
    AddUpdateCommand cmd = new AddUpdateCommand(makeReq(core));
    cmd.solrDoc = sdoc(fieldValues);
    updater.addDoc(cmd);
  }

  private LocalSolrQueryRequest makeReq(SolrCore core, String... q) {
    if (q.length == 1) {
      return new LocalSolrQueryRequest(core,
          q[0], null, 0, 20, new HashMap<String, String>());
    }
    if (q.length % 2 != 0) {
      throw new RuntimeException("The length of the string array (query arguments) needs to be even");
    }
    NamedList.NamedListEntry[] entries = new NamedList.NamedListEntry[q.length / 2];
    for (int i = 0; i < q.length; i += 2) {
      entries[i / 2] = new NamedList.NamedListEntry<String>(q[i], q[i + 1]);
    }
    return new LocalSolrQueryRequest(core, new NamedList<Object>(entries));
  }

  private final static String LOTS_SOLR_XML = " <solr persistent=\"false\"> " +
      "<cores adminPath=\"/admin/cores\" defaultCoreName=\"collectionLazy2\" transientCacheSize=\"4\">  " +
      "<core name=\"collection1\" instanceDir=\"collection1\" config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\" /> " +

      "<core name=\"collectionLazy2\" instanceDir=\"collection2\" transient=\"true\" loadOnStartup=\"true\"  " +
      " config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\" /> " +

      "<core name=\"collectionLazy3\" instanceDir=\"collection3\" transient=\"on\" loadOnStartup=\"false\" " +
      "config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\"  /> " +

      "<core name=\"collectionLazy4\" instanceDir=\"collection4\" transient=\"false\" loadOnStartup=\"false\" " +
      "config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\"  /> " +

      "<core name=\"collectionLazy5\" instanceDir=\"collection5\" transient=\"false\" loadOnStartup=\"true\" " +
      "config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\"  /> " +

      "<core name=\"collectionLazy6\" instanceDir=\"collection6\" transient=\"true\" loadOnStartup=\"false\" " +
      "config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\"  /> " +

      "<core name=\"collectionLazy7\" instanceDir=\"collection7\" transient=\"true\" loadOnStartup=\"false\" " +
      "config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\"  /> " +

      "<core name=\"collectionLazy8\" instanceDir=\"collection8\" transient=\"true\" loadOnStartup=\"false\" " +
      "config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\"  /> " +

      "<core name=\"collectionLazy9\" instanceDir=\"collection9\" transient=\"true\" loadOnStartup=\"false\" " +
      "config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\"  /> " +

      "</cores> " +
      "</solr>";
}
