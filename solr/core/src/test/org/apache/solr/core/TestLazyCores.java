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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TestLazyCores extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  private CoreContainer cores;

  @Before
  public void before() throws IOException, SAXException, ParserConfigurationException {
    String solrHome;
    solrHome = SolrResourceLoader.locateSolrHome();
    File fconf = new File(solrHome, "solr-lots-of-cores.xml");

    cores = new CoreContainer(solrHome);
    cores.load(solrHome, fconf);
    cores.setPersistent(false);
  }
  @Test
  public void testLazyLoad() {
    try {
      // NOTE: the way this works, this should not assert, however if it's put after the getCore on this collection,
      // that will cause the core to be loaded and this test will fail.

      Collection<String> names = cores.getCoreNames();
      for (String name : names) {
        assertFalse("collectionLazy2".equals(name));
      }

      SolrCore core1 = cores.getCore("collection1");
      CoreDescriptor cont = core1.getCoreDescriptor();
      assertFalse("core1 should not be swappable", core1.getCoreDescriptor().isSwappable());
      assertTrue("core1 should  be loadable", core1.getCoreDescriptor().isLoadOnStartup());
      assertNotNull(core1.getSolrConfig());

      SolrCore core2 = cores.getCore("collectionLazy2");
      assertTrue("core2 should not be swappable", core2.getCoreDescriptor().isSwappable());
      assertFalse("core2 should not be loadable", core2.getCoreDescriptor().isLoadOnStartup());

      SolrCore core3 = cores.getCore("collectionLazy3");
      assertTrue("core3 should not be swappable", core3.getCoreDescriptor().isSwappable());
      assertFalse("core3 should not be loadable", core3.getCoreDescriptor().isLoadOnStartup());

      SolrCore core4 = cores.getCore("collectionLazy4");
      assertFalse("core4 should not be swappable", core4.getCoreDescriptor().isSwappable());
      assertFalse("core4 should not be loadable", core4.getCoreDescriptor().isLoadOnStartup());

      SolrCore core5 = cores.getCore("collectionLazy5");
      assertFalse("core5 should not be swappable", core5.getCoreDescriptor().isSwappable());
      assertTrue("core5 should  be loadable", core5.getCoreDescriptor().isLoadOnStartup());

      core1.close();
      core2.close();
      core3.close();
      core4.close();
      core5.close();
    } finally {
      cores.shutdown();
    }
  }

  // This is a little weak. I'm not sure how to test that lazy core2 is loaded automagically. The getCore
  // will, of course, load it.
  @Test
  public void testLazySearch() throws Exception {
    try {
      // Make sure Lazy2 isn't loaded.
      checkNotInCores("collectionLazy2");
      SolrCore core2 = cores.getCore("collectionLazy2");

      addLazy(core2, "id", "0");
      addLazy(core2, "id", "1", "v_t", "Hello Dude");
      addLazy(core2, "id", "2", "v_t", "Hello Yonik");
      addLazy(core2, "id", "3", "v_s", "{!literal}");
      addLazy(core2, "id", "4", "v_s", "other stuff");
      addLazy(core2, "id", "5", "v_f", "3.14159");
      addLazy(core2, "id", "6", "v_f", "8983");

      SolrQueryRequest req = makeReq(core2);
      CommitUpdateCommand cmtCmd = new CommitUpdateCommand(req, false);
      core2.getUpdateHandler().commit(cmtCmd);

      RefCounted<SolrIndexSearcher> holder = core2.getSearcher();
      SolrIndexSearcher searcher = holder.get();

      // Just get a couple of searches to work!
      assertQ("test prefix query",
          makeReq(core2, "q", "{!prefix f=v_t}hel")
          , "//result[@numFound='2']"
      );

      assertQ("test raw query",
          makeReq(core2, "q", "{!raw f=v_t}hello")
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
          makeReq(core2, "q", "{!raw f=v_t}Hello")
          , "//result[@numFound='0']"
      );
      assertQ("test raw query",
          makeReq(core2, "q", "{!raw f=v_f}1.5")
          , "//result[@numFound='0']"
      );

      checkInCores("collectionLazy2");

      searcher.close();
      core2.close();
    } finally {
      cores.shutdown();
    }
  }
  @Test
  public void testCachingLimit() {
    try {
      // NOTE: the way this works, this should not assert, however if it's put after the getCore on this collection,
      // that will cause the core to be loaded and this test will fail.
      Collection<String> names = cores.getCoreNames();

      // By putting these in non-alpha order, we're also checking that we're  not just seeing an artifact.
      SolrCore core1 = cores.getCore("collection1");
      SolrCore core2 = cores.getCore("collectionLazy3");
      SolrCore core4 = cores.getCore("collectionLazy4");
      SolrCore core3 = cores.getCore("collectionLazy2");
      SolrCore core5 = cores.getCore("collectionLazy5");


      checkInCores("collection1", "collectionLazy2", "collectionLazy3", "collectionLazy4", "collectionLazy5");
      checkNotInCores("collectionLazy6", "collectionLazy7", "collectionLazy8", "collectionLazy9");

      // map should be full up, add one more and verify
      SolrCore core6 = cores.getCore("collectionLazy6");
      checkInCores("collection1", "collectionLazy2", "collectionLazy3", "collectionLazy4", "collectionLazy5", "collectionLazy6");
      checkNotInCores("collectionLazy7", "collectionLazy8", "collectionLazy9");

      SolrCore core7 = cores.getCore("collectionLazy7");
      checkInCores("collection1", "collectionLazy2", "collectionLazy3", "collectionLazy4", "collectionLazy5", "collectionLazy6", "collectionLazy7");
      checkNotInCores("collectionLazy8", "collectionLazy9");
      SolrCore core8 = cores.getCore("collectionLazy8");
      checkInCores("collection1", "collectionLazy2", "collectionLazy4", "collectionLazy5", "collectionLazy6", "collectionLazy7", "collectionLazy8");
      checkNotInCores("collectionLazy3", "collectionLazy9");

      SolrCore core9 = cores.getCore("collectionLazy9");
      checkInCores("collection1", "collectionLazy4", "collectionLazy5", "collectionLazy6", "collectionLazy7", "collectionLazy8", "collectionLazy9");
      checkNotInCores( "collectionLazy2","collectionLazy3");


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
      cores.shutdown();
    }
  }

  private void checkNotInCores(String... nameCheck) {
    Collection<String> names = cores.getCoreNames();
    for (String name : nameCheck) {
      assertFalse("core " + name + " was found in the list of cores", names.contains(name));
    }
  }

  private void checkInCores(String... nameCheck) {
    Collection<String> names = cores.getCoreNames();
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
    Map.Entry<String, String>[] entries = new NamedList.NamedListEntry[q.length / 2];
    for (int i = 0; i < q.length; i += 2) {
      entries[i / 2] = new NamedList.NamedListEntry<String>(q[i], q[i + 1]);
    }
    return new LocalSolrQueryRequest(core, new NamedList(entries));
  }
}
