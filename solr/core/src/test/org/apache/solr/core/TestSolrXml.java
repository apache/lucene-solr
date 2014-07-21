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

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Locale;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class TestSolrXml extends SolrTestCaseJ4 {

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  // tmp dir, cleanedup automaticly.
  private static File solrHome = null;
  private static SolrResourceLoader loader = null;

  @BeforeClass
  public static void setupLoader() throws Exception {
    solrHome = createTempDir();
    loader = new SolrResourceLoader(solrHome.getAbsolutePath());
  }

  @AfterClass
  public static void cleanupLoader() throws Exception {
    solrHome = null;
    loader = null;
  }

  public void testAllInfoPresent() throws IOException {

    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    FileUtils.copyFile(new File(testSrcRoot, "solr-50-all.xml"), new File(solrHome, "solr.xml"));

    ConfigSolr cfg = ConfigSolr.fromSolrHome(loader, solrHome.getAbsolutePath());
    
    assertEquals("core admin handler class", "testAdminHandler", cfg.getCoreAdminHandlerClass());
    assertEquals("collection handler class", "testCollectionsHandler", cfg.getCollectionsHandlerClass());
    assertEquals("info handler class", "testInfoHandler", cfg.getInfoHandlerClass());
    assertEquals("core load threads", 11, cfg.getCoreLoadThreadCount());
    assertEquals("core root dir", "testCoreRootDirectory", cfg.getCoreRootDirectory());
    assertEquals("distrib conn timeout", 22, cfg.getDistributedConnectionTimeout());
    assertEquals("distrib socket timeout", 33, cfg.getDistributedSocketTimeout());
    assertEquals("max update conn", 3, cfg.getMaxUpdateConnections());
    assertEquals("max update conn/host", 37, cfg.getMaxUpdateConnectionsPerHost());
    assertEquals("host", "testHost", cfg.getHost());
    assertEquals("zk host context", "testHostContext", cfg.getZkHostContext());
    assertEquals("zk host port", "44", cfg.getZkHostPort());
    assertEquals("leader vote wait", 55, cfg.getLeaderVoteWait());
    assertEquals("logging class", "testLoggingClass", cfg.getLogWatcherConfig().getLoggingClass());
    assertEquals("log watcher", true, cfg.getLogWatcherConfig().isEnabled());
    assertEquals("log watcher size", 88, cfg.getLogWatcherConfig().getWatcherSize());
    assertEquals("log watcher thresh", "99", cfg.getLogWatcherConfig().getWatcherThreshold());
    assertEquals("manage path", "testManagementPath", cfg.getManagementPath());
    assertEquals("shardLib", "testSharedLib", cfg.getSharedLibDirectory());
    assertEquals("schema cache", true, cfg.hasSchemaCache());
    assertEquals("trans cache size", 66, cfg.getTransientCacheSize());
    assertEquals("zk client timeout", 77, cfg.getZkClientTimeout());
    assertEquals("zk host", "testZkHost", cfg.getZkHost());
    assertEquals("persistent", true, cfg.isPersistent());
    assertEquals("core admin path", ConfigSolr.DEFAULT_CORE_ADMIN_PATH, cfg.getAdminPath());
  }

  // Test  a few property substitutions that happen to be in solr-50-all.xml.
  public void testPropertySub() throws IOException {

    System.setProperty("coreRootDirectory", "myCoreRoot");
    System.setProperty("hostPort", "8888");
    System.setProperty("shareSchema", "false");
    System.setProperty("socketTimeout", "220");
    System.setProperty("connTimeout", "200");

    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    FileUtils.copyFile(new File(testSrcRoot, "solr-50-all.xml"), new File(solrHome, "solr.xml"));

    ConfigSolr cfg = ConfigSolr.fromSolrHome(loader, solrHome.getAbsolutePath());
    assertEquals("core root dir", "myCoreRoot", cfg.getCoreRootDirectory());
    assertEquals("zk host port", "8888", cfg.getZkHostPort());
    assertEquals("schema cache", false, cfg.hasSchemaCache());
  }

  public void testExplicitNullGivesDefaults() throws IOException {
    // 2 diff options, one where the default is in fact null, and one where it isn't
    String solrXml = "<solr><solrcloud><null name=\"host\"/><null name=\"leaderVoteWait\"/></solrcloud></solr>";

    ConfigSolr cfg = ConfigSolr.fromString(loader, solrXml);
    assertEquals("host", null, cfg.getHost());
    assertEquals("leaderVoteWait", 180000, cfg.getLeaderVoteWait());
  }

  public void testIntAsLongBad() throws IOException {
    String bad = ""+TestUtil.nextLong(random(), Integer.MAX_VALUE, Long.MAX_VALUE);
    String solrXml = "<solr><long name=\"transientCacheSize\">"+bad+"</long></solr>";

    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "Value of '%s' can not be parsed as 'int'", bad));
    ConfigSolr cfg = ConfigSolr.fromString(loader, solrXml);
  }

  public void testIntAsLongOk() throws IOException {
    int ok = random().nextInt();
    String solrXml = "<solr><long name=\"transientCacheSize\">"+ok+"</long></solr>";
    ConfigSolr cfg = ConfigSolr.fromString(loader, solrXml);
    assertEquals(ok, cfg.getTransientCacheSize());
  }

  public void testMultiCloudSectionError() throws IOException {
    String solrXml = "<solr>"
      + "<solrcloud><bool name=\"genericCoreNodeNames\">true</bool></solrcloud>"
      + "<solrcloud><bool name=\"genericCoreNodeNames\">false</bool></solrcloud>"
      + "</solr>";
    expectedException.expect(SolrException.class);
    expectedException.expectMessage("2 instances of <solrcloud> found in solr.xml");
    ConfigSolr cfg = ConfigSolr.fromString(loader, solrXml);
  }

  public void testMultiLoggingSectionError() throws IOException {
    String solrXml = "<solr>"
      + "<logging><str name=\"class\">foo</str></logging>"
      + "<logging><str name=\"class\">foo</str></logging>"
      + "</solr>";
    expectedException.expect(SolrException.class);
    expectedException.expectMessage("2 instances of <logging> found in solr.xml");
    ConfigSolr cfg = ConfigSolr.fromString(loader, solrXml);
  }

  public void testMultiLoggingWatcherSectionError() throws IOException {
    String solrXml = "<solr><logging>"
      + "<watcher><int name=\"threshold\">42</int></watcher>"
      + "<watcher><int name=\"threshold\">42</int></watcher>"
      + "<watcher><int name=\"threshold\">42</int></watcher>"
      + "</logging></solr>";

    expectedException.expect(SolrException.class);
    expectedException.expectMessage("3 instances of Logging <watcher> found in solr.xml");
    ConfigSolr cfg = ConfigSolr.fromString(loader, solrXml);
  }
 
  public void testValidStringValueWhenBoolTypeIsExpected() throws IOException {
    boolean genericCoreNodeNames = random().nextBoolean();
    String solrXml = String.format(Locale.ROOT, "<solr><solrcloud><str name=\"genericCoreNodeNames\">%s</str></solrcloud></solr>", genericCoreNodeNames);

    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
    assertEquals("gen core node names", genericCoreNodeNames, configSolr.getGenericCoreNodeNames());
  }

  public void testValidStringValueWhenIntTypeIsExpected() throws IOException {
    int maxUpdateConnections = random().nextInt();
    String solrXml = String.format(Locale.ROOT, "<solr><solrcloud><str name=\"maxUpdateConnections\">%d</str></solrcloud></solr>", maxUpdateConnections);
    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
    assertEquals("max update conn", maxUpdateConnections, configSolr.getMaxUpdateConnections());
  }

  public void testFailAtConfigParseTimeWhenIntTypeIsExpectedAndLongTypeIsGiven() throws IOException {
    long val = TestUtil.nextLong(random(), Integer.MAX_VALUE, Long.MAX_VALUE);
    String solrXml = String.format(Locale.ROOT, "<solr><solrcloud><long name=\"maxUpdateConnections\">%d</long></solrcloud></solr>", val);

    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "Value of '%d' can not be parsed as 'int'", val));
    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenBoolTypeIsExpectedAndLongTypeIsGiven() throws IOException {
    long val = random().nextLong();
    String solrXml = String.format(Locale.ROOT, "<solr><solrcloud><long name=\"genericCoreNodeNames\">%d</long></solrcloud></solr>", val);

    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "Value of '%d' can not be parsed as 'bool'", val));
    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenBoolTypeIsExpectedAndDoubleTypeIsGiven() throws IOException {
    String val = ""+random().nextDouble();
    String solrXml = String.format(Locale.ROOT, "<solr><solrcloud><double name=\"genericCoreNodeNames\">%s</double></solrcloud></solr>", val);

    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "Value of '%s' can not be parsed as 'bool'", val));
    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenBoolTypeIsExpectedAndValueIsInvalidString() throws IOException {
    String solrXml = "<solr><solrcloud><bool name=\"genericCoreNodeNames\">NOT_A_BOOLEAN</bool></solrcloud></solr>";

    expectedException.expect(SolrException.class);
    expectedException.expectMessage("invalid boolean value: NOT_A_BOOLEAN");
    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenIntTypeIsExpectedAndBoolTypeIsGiven() throws IOException {
    // given:
    boolean randomBoolean = random().nextBoolean();
    String solrXml = String.format(Locale.ROOT, "<solr><logging><int name=\"unknown-option\">%s</int></logging></solr>", randomBoolean);

    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "Value of 'unknown-option' can not be parsed as 'int': \"%s\"", randomBoolean));

    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenUnrecognizedSolrCloudOptionWasFound() throws IOException {
    String solrXml = "<solr><solrcloud><bool name=\"unknown-option\">true</bool></solrcloud></solr>";

    expectedException.expect(SolrException.class);
    expectedException.expectMessage("<solrcloud> section of solr.xml contains 1 unknown config parameter(s): [unknown-option]");
    
    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenUnrecognizedSolrOptionWasFound() throws IOException {
    String solrXml = "<solr><bool name=\"unknown-bool-option\">true</bool><str name=\"unknown-str-option\">true</str></solr>";

    expectedException.expect(SolrException.class);
    expectedException.expectMessage("Main section of solr.xml contains 2 unknown config parameter(s): [unknown-bool-option, unknown-str-option]");

    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenUnrecognizedLoggingOptionWasFound() throws IOException {
    String solrXml = String.format(Locale.ROOT, "<solr><logging><bool name=\"unknown-option\">%s</bool></logging></solr>", random().nextBoolean());

    expectedException.expect(SolrException.class);
    expectedException.expectMessage("<logging> section of solr.xml contains 1 unknown config parameter(s): [unknown-option]");

    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenLoggingConfigParamsAreDuplicated() throws IOException {
    String v1 = ""+random().nextInt();
    String v2 = ""+random().nextInt();
    String solrXml = String.format(Locale.ROOT,
                                   "<solr><logging>" +
                                   "<str name=\"class\">%s</str>" +
                                   "<str name=\"class\">%s</str>" +
                                   "</logging></solr>",
                                   v1, v2);

    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "<logging> section of solr.xml contains duplicated 'class' in solr.xml: [%s, %s]", v1, v2));

    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenSolrCloudConfigParamsAreDuplicated() throws IOException {
    String v1 = ""+random().nextInt();
    String v2 = ""+random().nextInt();
    String v3 = ""+random().nextInt();
    String solrXml = String.format(Locale.ROOT,
                                   "<solr><solrcloud>" +
                                   "<int name=\"zkClientTimeout\">%s</int>" +
                                   "<int name=\"zkClientTimeout\">%s</int>" +
                                   "<str name=\"zkHost\">foo</str>" + // other ok val in middle
                                   "<int name=\"zkClientTimeout\">%s</int>" +
                                   "</solrcloud></solr>",
                                   v1, v2, v3);
    
    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "<solrcloud> section of solr.xml contains duplicated 'zkClientTimeout' in solr.xml: [%s, %s, %s]", v1, v2, v3));

    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }

  public void testFailAtConfigParseTimeWhenSolrConfigParamsAreDuplicated() throws IOException {
    String v1 = ""+random().nextInt();
    String v2 = ""+random().nextInt();
    String solrXml = String.format(Locale.ROOT, 
                                   "<solr>" +
                                   "<int name=\"coreLoadThreads\">%s</int>" +
                                   "<str name=\"coreLoadThreads\">%s</str>" +
                                   "</solr>",
                                   v1, v2);

    expectedException.expect(SolrException.class);
    expectedException.expectMessage(String.format(Locale.ROOT, "Main section of solr.xml contains duplicated 'coreLoadThreads' in solr.xml: [%s, %s]", v1, v2));

    ConfigSolr configSolr = ConfigSolr.fromString(loader, solrXml);
  }
}

