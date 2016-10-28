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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.ReadOnlyCoresLocator;
import org.junit.Test;

public class TestLazyCores extends SolrTestCaseJ4 {

  private File solrHomeDirectory;

  private static CoreDescriptor makeCoreDescriptor(CoreContainer cc, String coreName, String isTransient, String loadOnStartup) {
    return new CoreDescriptor(cc, coreName, cc.getCoreRootDirectory().resolve(coreName),
        CoreDescriptor.CORE_TRANSIENT, isTransient,
        CoreDescriptor.CORE_LOADONSTARTUP, loadOnStartup);
  }

  private static CoresLocator testCores = new ReadOnlyCoresLocator() {
    @Override
    public List<CoreDescriptor> discover(CoreContainer cc) {
      return ImmutableList.of(
          makeCoreDescriptor(cc, "collection1", "false", "true"),
          makeCoreDescriptor(cc, "collection2", "true", "true"),
          makeCoreDescriptor(cc, "collection3", "on", "false"),
          makeCoreDescriptor(cc, "collection4", "false", "false"),
          makeCoreDescriptor(cc, "collection5", "false", "true"),
          makeCoreDescriptor(cc, "collection6", "true", "false"),
          makeCoreDescriptor(cc, "collection7", "true", "false"),
          makeCoreDescriptor(cc, "collection8", "true", "false"),
          makeCoreDescriptor(cc, "collection9", "true", "false")
      );
    }
  };


  private CoreContainer init() throws Exception {
    solrHomeDirectory = createTempDir().toFile();
    
    for (int idx = 1; idx < 10; ++idx) {
      copyMinConf(new File(solrHomeDirectory, "collection" + idx));
    }

    SolrResourceLoader loader = new SolrResourceLoader(solrHomeDirectory.toPath());
    NodeConfig config = new NodeConfig.NodeConfigBuilder("testNode", loader).setTransientCacheSize(4).build();
    return createCoreContainer(config, testCores);
  }
  
  @Test
  public void testLazyLoad() throws Exception {
    CoreContainer cc = init();
    try {

      // NOTE: This checks the initial state for loading, no need to do this elsewhere.
      checkInCores(cc, "collection1", "collection2", "collection5");
      checkNotInCores(cc, "collection3", "collection4", "collection6", "collection7",
          "collection8", "collection9");

      SolrCore core1 = cc.getCore("collection1");
      assertFalse("core1 should not be transient", core1.getCoreDescriptor().isTransient());
      assertTrue("core1 should be loadable", core1.getCoreDescriptor().isLoadOnStartup());
      assertNotNull(core1.getSolrConfig());

      SolrCore core2 = cc.getCore("collection2");
      assertTrue("core2 should be transient", core2.getCoreDescriptor().isTransient());
      assertTrue("core2 should be loadable", core2.getCoreDescriptor().isLoadOnStartup());

      SolrCore core3 = cc.getCore("collection3");
      assertTrue("core3 should be transient", core3.getCoreDescriptor().isTransient());
      assertFalse("core3 should not be loadable", core3.getCoreDescriptor().isLoadOnStartup());

      SolrCore core4 = cc.getCore("collection4");
      assertFalse("core4 should not be transient", core4.getCoreDescriptor().isTransient());
      assertFalse("core4 should not be loadable", core4.getCoreDescriptor().isLoadOnStartup());

      SolrCore core5 = cc.getCore("collection5");
      assertFalse("core5 should not be transient", core5.getCoreDescriptor().isTransient());
      assertTrue("core5 should be loadable", core5.getCoreDescriptor().isLoadOnStartup());

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

  private void checkSearch(SolrCore core) throws IOException {
    addLazy(core, "id", "0");
    addLazy(core, "id", "1", "v_t", "Hello Dude");
    addLazy(core, "id", "2", "v_t", "Hello Yonik");
    addLazy(core, "id", "3", "v_s", "{!literal}");
    addLazy(core, "id", "4", "v_s", "other stuff");
    addLazy(core, "id", "5", "v_f", "3.14159");
    addLazy(core, "id", "6", "v_f", "8983");

    SolrQueryRequest req = makeReq(core);
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(req, false);
    core.getUpdateHandler().commit(cmtCmd);

    // Just get a couple of searches to work!
    assertQ("test prefix query",
        makeReq(core, "q", "{!prefix f=v_t}hel", "wt", "xml")
        , "//result[@numFound='2']"
    );

    assertQ("test raw query",
        makeReq(core, "q", "{!raw f=v_t}hello", "wt", "xml")
        , "//result[@numFound='2']"
    );

    // no analysis is done, so these should match nothing
    assertQ("test raw query",
        makeReq(core, "q", "{!raw f=v_t}Hello", "wt", "xml")
        , "//result[@numFound='0']"
    );
    assertQ("test raw query",
        makeReq(core, "q", "{!raw f=v_f}1.5", "wt", "xml")
        , "//result[@numFound='0']"
    );
  }
  @Test
  public void testLazySearch() throws Exception {
    CoreContainer cc = init();
    try {
      // Make sure Lazy4 isn't loaded. Should be loaded on the get
      checkNotInCores(cc, "collection4");
      SolrCore core4 = cc.getCore("collection4");

      checkSearch(core4);

      // Now just insure that the normal searching on "collection1" finds _0_ on the same query that found _2_ above.
      // Use of makeReq above and req below is tricky, very tricky.
      SolrCore collection1 = cc.getCore("collection1");
      assertQ("test raw query",
          makeReq(collection1, "q", "{!raw f=v_t}hello", "wt", "xml")
          , "//result[@numFound='0']"
      );

      checkInCores(cc, "collection4");

      core4.close();
      collection1.close();
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testCachingLimit() throws Exception {
    CoreContainer cc = init();
    try {
      // First check that all the cores that should be loaded at startup actually are.

      checkInCores(cc, "collection1", "collection2", "collection5");
      checkNotInCores(cc, "collection3", "collection4", "collection6", "collection7", "collection8", "collection9");

      // By putting these in non-alpha order, we're also checking that we're  not just seeing an artifact.
      SolrCore core1 = cc.getCore("collection1");
      SolrCore core3 = cc.getCore("collection3");
      SolrCore core4 = cc.getCore("collection4");
      SolrCore core2 = cc.getCore("collection2");
      SolrCore core5 = cc.getCore("collection5");

      checkInCores(cc, "collection1", "collection2", "collection3", "collection4", "collection5");
      checkNotInCores(cc, "collection6", "collection7", "collection8", "collection9");

      // map should be full up, add one more and verify
      SolrCore core6 = cc.getCore("collection6");
      checkInCores(cc, "collection1", "collection2", "collection3", "collection4", "collection5",
          "collection6");
      checkNotInCores(cc, "collection7", "collection8", "collection9");

      SolrCore core7 = cc.getCore("collection7");
      checkInCores(cc, "collection1", "collection2", "collection3", "collection4", "collection5",
          "collection6", "collection7");
      checkNotInCores(cc, "collection8", "collection9");

      SolrCore core8 = cc.getCore("collection8");
      checkInCores(cc, "collection1", "collection2", "collection4", "collection5", "collection6",
          "collection7", "collection8");
      checkNotInCores(cc, "collection3", "collection9");

      SolrCore core9 = cc.getCore("collection9");
      checkInCores(cc, "collection1", "collection4", "collection5", "collection6", "collection7",
          "collection8", "collection9");
      checkNotInCores(cc, "collection2", "collection3");


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
    final List<SolrCore> theCores = new ArrayList<>();
    final CoreContainer cc = init();
    try {

      Thread[] threads = new Thread[15];
      for (int idx = 0; idx < threads.length; idx++) {
        threads[idx] = new Thread() {
          @Override
          public void run() {
            SolrCore core = cc.getCore("collection3");
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
          "schema", "schema.xml",
          "config", "solrconfig.xml");

      admin.handleRequestBody(request, resp);
      fail("Should have thrown an error");
    } catch (SolrException se) {
      //SolrException cause = (SolrException)se.getCause();
      assertEquals("Exception code should be 500", 500, se.code());
      for (String err : errs) {
       assertTrue("Should have seen an exception containing the an error",
            se.getMessage().contains(err));
      }
    }
  }
  @Test
  public void testCreateSame() throws Exception {
    final CoreContainer cc = init();
    try {
      // First, try all 4 combinations of load on startup and transient
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      SolrCore lc2 = cc.getCore("collection2");
      SolrCore lc4 = cc.getCore("collection4");
      SolrCore lc5 = cc.getCore("collection5");
      SolrCore lc6 = cc.getCore("collection6");

      copyMinConf(new File(solrHomeDirectory, "t2"));
      copyMinConf(new File(solrHomeDirectory, "t4"));
      copyMinConf(new File(solrHomeDirectory, "t5"));
      copyMinConf(new File(solrHomeDirectory, "t6"));


      // Should also fail with the same name
      tryCreateFail(admin, "collection2", "t12", "Core with name", "collection2", "already exists");
      tryCreateFail(admin, "collection4", "t14", "Core with name", "collection4", "already exists");
      tryCreateFail(admin, "collection5", "t15", "Core with name", "collection5", "already exists");
      tryCreateFail(admin, "collection6", "t16", "Core with name", "collection6", "already exists");

      lc2.close();
      lc4.close();
      lc5.close();
      lc6.close();

    } finally {
      cc.shutdown();
    }
  }

  private void createViaAdmin(CoreContainer cc, String name, boolean isTransient,
                              boolean loadOnStartup) throws Exception {

    final CoreAdminHandler admin = new CoreAdminHandler(cc);
    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.CREATE.toString(),
            CoreAdminParams.NAME, name,
            CoreAdminParams.TRANSIENT, Boolean.toString(isTransient),
            CoreAdminParams.LOAD_ON_STARTUP, Boolean.toString(loadOnStartup)),
            resp);

  }

  private void unloadViaAdmin(CoreContainer cc, String name) throws Exception {

    final CoreAdminHandler admin = new CoreAdminHandler(cc);
    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.UNLOAD.toString(),
            CoreAdminParams.CORE, name),
            resp);

  }


  // Make sure that creating a transient core from the admin handler correctly respects the transient limits etc.
  @Test
  public void testCreateTransientFromAdmin() throws Exception {
    final CoreContainer cc = init();
    try {
      copyMinConf(new File(solrHomeDirectory, "core1"));
      copyMinConf(new File(solrHomeDirectory, "core2"));
      copyMinConf(new File(solrHomeDirectory, "core3"));
      copyMinConf(new File(solrHomeDirectory, "core4"));
      copyMinConf(new File(solrHomeDirectory, "core5"));

      createViaAdmin(cc, "core1", true, true);
      createViaAdmin(cc, "core2", true, false);
      createViaAdmin(cc, "core3", true, true);
      createViaAdmin(cc, "core4", true, false);
      createViaAdmin(cc, "core5", true, false);

      final SolrCore c1 = cc.getCore("core1");
      final SolrCore c2 = cc.getCore("core2");
      final SolrCore c3 = cc.getCore("core3");
      final SolrCore c4 = cc.getCore("core4");
      final SolrCore c5 = cc.getCore("core5");

      checkNotInCores(cc, "core1", "collection2", "collection3", "collection4", "collection6"
          , "collection7", "collection8", "collection9");

      checkInCores(cc, "collection1", "collection5", "core2", "core3", "core4", "core5");

      // While we're at it, a test for SOLR-5366, unloading transient core that's been unloaded b/c it's
      // transient generates a "too many closes" errorl

      class TestThread extends Thread {
        
        @Override
        public void run() {
          
          final int sleep_millis = random().nextInt(1000);
          try {
            if (sleep_millis > 0) {
              if (VERBOSE) {
                System.out.println("TestLazyCores.testCreateTransientFromAdmin Thread.run sleeping for "+sleep_millis+" ms");
              }
              Thread.sleep(sleep_millis);
            }
          }
          catch (InterruptedException ie) {
            if (VERBOSE) {
              System.out.println("TestLazyCores.testCreateTransientFromAdmin Thread.run caught "+ie+" whilst sleeping for "+sleep_millis+" ms");
            }
          }

          c1.close();
          c2.close();
          c3.close();
          c4.close();
          c5.close();
        }
      };
      
      // with SOLR-6279 UNLOAD will wait for the core's reference count to have reached zero
      // hence cN.close() need to proceed or run in parallel with unloadViaAdmin(...)
      final TestThread cThread = new TestThread();
      cThread.start();
      
      unloadViaAdmin(cc, "core1");
      unloadViaAdmin(cc, "core2");
      unloadViaAdmin(cc, "core3");
      unloadViaAdmin(cc, "core4");
      unloadViaAdmin(cc, "core5");

      cThread.join();

    } finally {
      cc.shutdown();
    }
  }

  // Test that transient cores
  // 1> produce errors as appropriate when the config or schema files are foo'd
  // 2> "self heal". That is, if the problem is corrected can the core be reloaded and used?
  // 3> that OK cores can be searched even when some cores failed to load.
  @Test
  public void testBadConfigsGenerateErrors() throws Exception {
    final CoreContainer cc = initGoodAndBad(Arrays.asList("core1", "core2"),
        Arrays.asList("badSchema1", "badSchema2"),
        Arrays.asList("badConfig1", "badConfig2"));
    try {
      // first, did the two good cores load successfully?
      checkInCores(cc, "core1", "core2");

      // Did the bad cores fail to load?
      checkNotInCores(cc, "badSchema1", "badSchema2", "badConfig1", "badConfig2");

      //  Can we still search the "good" cores even though there were core init failures?
      SolrCore core1 = cc.getCore("core1");
      checkSearch(core1);

      // Did we get the expected message for each of the cores that failed to load? Make sure we don't run afoul of
      // the dreaded slash/backslash difference on Windows and *nix machines.
      testMessage(cc.getCoreInitFailures(), makePath("badConfig1", "conf", "solrconfig.xml"));
      testMessage(cc.getCoreInitFailures(), makePath("badConfig2", "conf", "solrconfig.xml"));
      testMessage(cc.getCoreInitFailures(), makePath("badSchema1", "conf", "schema.xml"));
      testMessage(cc.getCoreInitFailures(), makePath("badSchema2", "conf", "schema.xml"));

      // Status should report that there are failure messages for the bad cores and none for the good cores.
      checkStatus(cc, true, "core1");
      checkStatus(cc, true, "core2");
      checkStatus(cc, false, "badSchema1");
      checkStatus(cc, false, "badSchema2");
      checkStatus(cc, false, "badConfig1");
      checkStatus(cc, false, "badConfig2");

      // Copy good config and schema files in and see if you can then load them (they are transient after all)
      copyGoodConf("badConfig1", "solrconfig-minimal.xml", "solrconfig.xml");
      copyGoodConf("badConfig2", "solrconfig-minimal.xml", "solrconfig.xml");
      copyGoodConf("badSchema1", "schema-tiny.xml", "schema.xml");
      copyGoodConf("badSchema2", "schema-tiny.xml", "schema.xml");

      // This should force a reload of the cores.
      SolrCore bc1 = cc.getCore("badConfig1");
      SolrCore bc2 = cc.getCore("badConfig2");
      SolrCore bs1 = cc.getCore("badSchema1");
      SolrCore bs2 = cc.getCore("badSchema2");

      // all the cores should be found in the list now.
      checkInCores(cc, "core1", "core2", "badSchema1", "badSchema2", "badConfig1", "badConfig2");

      // Did we clear out the errors by putting good files in place? And the cores that never were bad should be OK too.
      checkStatus(cc, true, "core1");
      checkStatus(cc, true, "core2");
      checkStatus(cc, true, "badSchema1");
      checkStatus(cc, true, "badSchema2");
      checkStatus(cc, true, "badConfig1");
      checkStatus(cc, true, "badConfig2");

      // Are the formerly bad cores now searchable? Testing one of each should do.
      checkSearch(core1);
      checkSearch(bc1);
      checkSearch(bs1);

      core1.close();
      bc1.close();
      bc2.close();
      bs1.close();
      bs2.close();
    } finally {
      cc.shutdown();
    }
  }

  // See fi the message you expect is in the list of failures
  private void testMessage(Map<String, CoreContainer.CoreLoadFailure> failures, String lookFor) {
    List<String> messages = new ArrayList<>();
    for (CoreContainer.CoreLoadFailure e : failures.values()) {
      String message = e.exception.getCause().getMessage();
      messages.add(message);
      if (message.contains(lookFor)) return;
    }
    fail("Should have found message containing these tokens " + lookFor + " in the failure messages: " + messages);
  }

  // Just localizes writing a configuration rather than repeating it for good and bad files.
  private void writeCustomConfig(String coreName, String config, String schema, String rand_snip) throws IOException {

    File coreRoot = new File(solrHomeDirectory, coreName);
    File subHome = new File(coreRoot, "conf");
    if (!coreRoot.exists()) {
      assertTrue("Failed to make subdirectory ", coreRoot.mkdirs());
    }
    // Write the file for core discovery
    FileUtils.writeStringToFile(new File(coreRoot, "core.properties"), "name=" + coreName +
        System.getProperty("line.separator") + "transient=true" +
        System.getProperty("line.separator") + "loadOnStartup=true", StandardCharsets.UTF_8);

    FileUtils.writeStringToFile(new File(subHome, "solrconfig.snippet.randomindexconfig.xml"), rand_snip, StandardCharsets.UTF_8);

    FileUtils.writeStringToFile(new File(subHome, "solrconfig.xml"), config, StandardCharsets.UTF_8);

    FileUtils.writeStringToFile(new File(subHome, "schema.xml"), schema, StandardCharsets.UTF_8);
  }

  // Write out the cores' config files, both bad schema files, bad config files as well as some good cores.
  private CoreContainer initGoodAndBad(List<String> goodCores,
                                       List<String> badSchemaCores,
                                       List<String> badConfigCores) throws Exception {
    solrHomeDirectory = createTempDir().toFile();
    
    // Don't pollute the log with exception traces when they're expected.
    ignoreException(Pattern.quote("SAXParseException"));
    
    // Create the cores that should be fine.
    for (String coreName : goodCores) {
      File coreRoot = new File(solrHomeDirectory, coreName);
      copyMinConf(coreRoot, "name=" + coreName);

    }

    // Collect the files that we'll write to the config directories.
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    String min_schema = FileUtils.readFileToString(new File(top, "schema-tiny.xml"),
        StandardCharsets.UTF_8);
    String min_config = FileUtils.readFileToString(new File(top, "solrconfig-minimal.xml"),
        StandardCharsets.UTF_8);
    String rand_snip = FileUtils.readFileToString(new File(top, "solrconfig.snippet.randomindexconfig.xml"),
        StandardCharsets.UTF_8);

    // Now purposely mess up the config files, introducing stupid syntax errors.
    String bad_config = min_config.replace("<requestHandler", "<reqsthalr");
    String bad_schema = min_schema.replace("<field", "<filed");

    // Create the cores with bad configs
    for (String coreName : badConfigCores) {
      writeCustomConfig(coreName, bad_config, min_schema, rand_snip);
    }

    // Create the cores with bad schemas.
    for (String coreName : badSchemaCores) {
      writeCustomConfig(coreName, min_config, bad_schema, rand_snip);
    }

    SolrResourceLoader loader = new SolrResourceLoader(solrHomeDirectory.toPath());
    NodeConfig config = SolrXmlConfig.fromString(loader, "<solr/>");

    // OK this should succeed, but at the end we should have recorded a series of errors.
    return createCoreContainer(config, new CorePropertiesLocator(config.getCoreRootDirectory()));
  }

  // We want to see that the core "heals itself" if an un-corrupted file is written to the directory.
  private void copyGoodConf(String coreName, String srcName, String dstName) throws IOException {
    File coreRoot = new File(solrHomeDirectory, coreName);
    File subHome = new File(coreRoot, "conf");
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, srcName), new File(subHome, dstName));

  }

  // If ok==true, we shouldn't be seeing any failure cases.
  // if ok==false, the core being examined should have a failure in the list.
  private void checkStatus(CoreContainer cc, Boolean ok, String core) throws Exception {
    SolrQueryResponse resp = new SolrQueryResponse();
    final CoreAdminHandler admin = new CoreAdminHandler(cc);
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.STATUS.toString(),
            CoreAdminParams.CORE, core),
            resp);

    Map<String, Exception> failures =
        (Map<String, Exception>) resp.getValues().get("initFailures");

    if (ok) {
      if (failures.size() != 0) {
        fail("Should have cleared the error, but there are failues " + failures.toString());
      }
    } else {
      if (failures.size() == 0) {
        fail("Should have had errors here but the status return has no failures!");
      }
    }
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
    cmd.solrDoc = sdoc((Object[])fieldValues);
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
      entries[i / 2] = new NamedList.NamedListEntry<>(q[i], q[i + 1]);
    }
    return new LocalSolrQueryRequest(core, new NamedList<>(entries));
  }

  private static final String makePath(String... args) {
    return StringUtils.join(args, File.separator);
  }

  @Test
  public void testMidUseUnload() throws Exception {
    final int maximumSleepMillis = random().nextInt(9999) + 1; // sleep for up to 10 s Must add 1 because using
                                                               // this as a seed will rea few lines down will
                                                               // throw an exception if this is zero
    if (VERBOSE) {
      System.out.println("TestLazyCores.testMidUseUnload maximumSleepMillis="+maximumSleepMillis);
    }
    
    class TestThread extends Thread {
      
      SolrCore core_to_use = null;
      
      @Override
      public void run() {
        
        final int sleep_millis = random().nextInt(maximumSleepMillis);
        try {
          if (sleep_millis > 0) {
            if (VERBOSE) {
              System.out.println("TestLazyCores.testMidUseUnload Thread.run sleeping for "+sleep_millis+" ms");
            }
            Thread.sleep(sleep_millis);
          }
        }
        catch (InterruptedException ie) {
          if (VERBOSE) {
            System.out.println("TestLazyCores.testMidUseUnload Thread.run caught "+ie+" whilst sleeping for "+sleep_millis+" ms");
          }
        }
        
        assertFalse(core_to_use.isClosed()); // not closed since we are still using it and hold a reference
        core_to_use.close(); // now give up our reference to the core
      }
    };

    CoreContainer cc = init();
    
    try {
      TestThread thread = new TestThread();
      
      thread.core_to_use = cc.getCore("collection1");
      assertNotNull(thread.core_to_use);
      assertFalse(thread.core_to_use.isClosed()); // freshly-in-use core is not closed
      thread.start();
      
      unloadViaAdmin(cc, "collection1");
      assertTrue(thread.core_to_use.isClosed()); // after unload-ing the core is closed

      thread.join();
    } finally {
      cc.shutdown();
    }
  }

}
