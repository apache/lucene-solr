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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.RefCounted;
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
    initCore("solrconfig.xml", "schema.xml");
  }

  private final File _solrHomeDirectory = new File(TEMP_DIR, "org.apache.solr.core.TestLazyCores_testlazy");

  private static String[] _necessaryConfs = {"schema.xml", "solrconfig.xml", "stopwords.txt", "synonyms.txt",
      "protwords.txt", "old_synonyms.txt", "currency.xml", "open-exchange-rates.json", "mapping-ISOLatin1Accent.txt"};

  private void copyConfFiles(File home, String subdir) throws IOException {

    File subHome = new File(new File(home, subdir), "conf");
    assertTrue("Failed to make subdirectory ", subHome.mkdirs());
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    for (String file : _necessaryConfs) {
      FileUtils.copyFile(new File(top, file), new File(subHome, file));
    }
  }

  private CoreContainer init() throws Exception {

    if (_solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(_solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", _solrHomeDirectory.mkdirs());
    for (int idx = 1; idx < 10; ++idx) {
      copyConfFiles(_solrHomeDirectory, "collection" + idx);
    }

    File solrXml = new File(_solrHomeDirectory, "solr.xml");
    FileUtils.write(solrXml, LOTS_SOLR_XML, IOUtils.CHARSET_UTF_8.toString());
    final CoreContainer cores = new CoreContainer(_solrHomeDirectory.getAbsolutePath());
    cores.load(_solrHomeDirectory.getAbsolutePath(), solrXml);
    //  h.getCoreContainer().load(_solrHomeDirectory.getAbsolutePath(), new File(_solrHomeDirectory, "solr.xml"));

    cores.setPersistent(false);
    return cores;
  }

  public void after() throws Exception {
    if (_solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(_solrHomeDirectory);
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
          makeReq(core4, "q", "{!prefix f=v_t}hel")
          , "//result[@numFound='2']"
      );

      assertQ("test raw query",
          makeReq(core4, "q", "{!raw f=v_t}hello")
          , "//result[@numFound='2']"
      );

      // Now just insure that the normal searching on "collection1" finds _0_ on the same query that found _2_ above.
      // Use of makeReq above and req below is tricky, very tricky.
      assertQ("test raw query",
          req("q", "{!raw f=v_t}hello")
          , "//result[@numFound='0']"
      );

      // no analysis is done, so these should match nothing
      assertQ("test raw query",
          makeReq(core4, "q", "{!raw f=v_t}Hello")
          , "//result[@numFound='0']"
      );
      assertQ("test raw query",
          makeReq(core4, "q", "{!raw f=v_f}1.5")
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

      checkInCores(cc, "collection1",  "collectionLazy2", "collectionLazy5");
      checkNotInCores(cc,"collectionLazy3", "collectionLazy4", "collectionLazy6",
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

  static List<SolrCore> _theCores = new ArrayList<SolrCore>();
  // Test case for SOLR-4300
  @Test
  public void testRace() throws Exception {
    final CoreContainer cc = init();
    try {

      Thread[] threads = new Thread[15];
      for (int idx = 0; idx < threads.length; idx++) {
        threads[idx] = new Thread() {
          @Override
          public void run() {
            SolrCore core = cc.getCore("collectionLazy3");
            synchronized (_theCores) {
              _theCores.add(core);
            }
          }
        };
        threads[idx].start();
      }

      for (Thread thread : threads) {
        thread.join();
      }

      for (int idx = 0; idx < _theCores.size() - 1; ++idx) {
        assertEquals("Cores should be the same!", _theCores.get(idx), _theCores.get(idx + 1));
      }

      for (SolrCore core : _theCores) {
        core.close();
      }

    } finally {
      cc.shutdown();
    }
  }

  private void checkNotInCores(CoreContainer cc, String... nameCheck) {
    Collection<String> names = cc.getCoreNames();
    for (String name : nameCheck) {
      assertFalse("core " + name + " was found in the list of cores", names.contains(name));
    }
  }

  private void checkInCores(CoreContainer cc, String... nameCheck) {
    Collection<String> names = cc.getCoreNames();
    for (String name : nameCheck) {
      assertTrue("core " + name + " was not found in the list of cores", names.contains(name));
    }
  }


  private void addLazy(SolrCore core, String... fieldValues) throws IOException {
    UpdateHandler updater = core.getUpdateHandler();
    SolrQueryRequest req = makeReq(core);
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    if ((fieldValues.length % 2) != 0) {
      throw new RuntimeException("The length of the string array (query arguments) needs to be even");
    }
    cmd.solrDoc = new SolrInputDocument();
    for (int idx = 0; idx < fieldValues.length; idx += 2) {
      cmd.solrDoc.addField(fieldValues[idx], fieldValues[idx + 1]);
    }

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
      "<core name=\"collection1\" instanceDir=\"collection1\" /> " +
      "<core name=\"collectionLazy2\" instanceDir=\"collection2\" transient=\"true\" loadOnStartup=\"true\"  /> " +
      "<core name=\"collectionLazy3\" instanceDir=\"collection3\" transient=\"on\" loadOnStartup=\"false\"/> " +
      "<core name=\"collectionLazy4\" instanceDir=\"collection4\" transient=\"false\" loadOnStartup=\"false\"/> " +
      "<core name=\"collectionLazy5\" instanceDir=\"collection5\" transient=\"false\" loadOnStartup=\"true\"/> " +
      "<core name=\"collectionLazy6\" instanceDir=\"collection6\" transient=\"true\" loadOnStartup=\"false\" /> " +
      "<core name=\"collectionLazy7\" instanceDir=\"collection7\" transient=\"true\" loadOnStartup=\"false\" /> " +
      "<core name=\"collectionLazy8\" instanceDir=\"collection8\" transient=\"true\" loadOnStartup=\"false\" /> " +
      "<core name=\"collectionLazy9\" instanceDir=\"collection9\" transient=\"true\" loadOnStartup=\"false\" /> " +
      "</cores> " +
      "</solr>";
}
