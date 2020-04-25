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
package org.apache.solr.cloud;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: This test would be a lot faster if it used a solrhome with fewer config
// files - there are a lot of them to upload
public class ZkCLITest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected ZkTestServer zkServer;

  protected Path zkDir;

  private String solrHome;

  private SolrZkClient zkClient;

  protected static final String SOLR_HOME = SolrTestCaseJ4.TEST_HOME();

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    log.info("####SETUP_START " + getTestName());

    String exampleHome = SolrJettyTestBase.legacyExampleCollection1SolrHome();

    Path tmpDir = createTempDir();
    solrHome = exampleHome;

    zkDir = tmpDir.resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:" + zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();


    this.zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT);

    log.info("####SETUP_END " + getTestName());
  }

  @Test
  public void testCmdConstants() throws Exception {
    assertEquals("upconfig", ZkCLI.UPCONFIG);
    assertEquals("x", ZkCLI.EXCLUDE_REGEX_SHORT);
    assertEquals("excluderegex", ZkCLI.EXCLUDE_REGEX);
    assertEquals(ZkConfigManager.UPLOAD_FILENAME_EXCLUDE_REGEX, ZkCLI.EXCLUDE_REGEX_DEFAULT);
  }

  @Test
  public void testBootstrapWithChroot() throws Exception {
    String chroot = "/foo/bar";
    assertFalse(zkClient.exists(chroot, true));

    String[] args = new String[] {"-zkhost", zkServer.getZkAddress() + chroot,
        "-cmd", "bootstrap", "-solrhome", this.solrHome};

    ZkCLI.main(args);

    assertTrue(zkClient.exists(chroot + ZkConfigManager.CONFIGS_ZKNODE
        + "/collection1", true));
  }

  @Test
  public void testMakePath() throws Exception {
    // test bootstrap_conf
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "makepath", "/path/mynewpath"};
    ZkCLI.main(args);


    assertTrue(zkClient.exists("/path/mynewpath", true));
  }

  @Test
  public void testPut() throws Exception {
    // test put
    String data = "my data";
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "put", "/data.txt", data};
    ZkCLI.main(args);

    zkClient.getData("/data.txt", null, null, true);

    assertArrayEquals(zkClient.getData("/data.txt", null, null, true), data.getBytes(StandardCharsets.UTF_8));

    // test re-put to existing
    data = "my data deux";
    args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "put", "/data.txt", data};
    ZkCLI.main(args);
    assertArrayEquals(zkClient.getData("/data.txt", null, null, true), data.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testPutFile() throws Exception {
    // test put file
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "putfile", "/solr.xml", SOLR_HOME + File.separator + "solr-stress-new.xml"};
    ZkCLI.main(args);

    String fromZk = new String(zkClient.getData("/solr.xml", null, null, true), StandardCharsets.UTF_8);
    File locFile = new File(SOLR_HOME + File.separator + "solr-stress-new.xml");
    InputStream is = new FileInputStream(locFile);
    String fromLoc;
    try {
      fromLoc = new String(IOUtils.toByteArray(is), StandardCharsets.UTF_8);
    } finally {
      IOUtils.closeQuietly(is);
    }
    assertEquals("Should get back what we put in ZK", fromZk, fromLoc);
  }

  @Test
  public void testPutFileNotExists() throws Exception {
    // test put file
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "putfile", "/solr.xml", SOLR_HOME + File.separator + "not-there.xml"};
    FileNotFoundException e = expectThrows(FileNotFoundException.class, () -> ZkCLI.main(args));
    assertTrue("Didn't find expected error message containing 'not-there.xml' in " + e.getMessage(),
        e.getMessage().indexOf("not-there.xml") != -1);
  }

  @Test
  public void testList() throws Exception {
    zkClient.makePath("/test", true);
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "list"};

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    final PrintStream myOut = new PrintStream(byteStream, false, StandardCharsets.UTF_8.name());
    ZkCLI.setStdout(myOut);

    ZkCLI.main(args);

    final String standardOutput = byteStream.toString(StandardCharsets.UTF_8.name());
    String separator = System.lineSeparator();
    assertEquals("/ (1)" + separator + " /test (0)" + separator + separator, standardOutput);
  }

  @Test
  public void testLs() throws Exception {
    zkClient.makePath("/test/path", true);
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "ls", "/test"};

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    final PrintStream myOut = new PrintStream(byteStream, false, StandardCharsets.UTF_8.name());
    ZkCLI.setStdout(myOut);

    ZkCLI.main(args);

    final String standardOutput = byteStream.toString(StandardCharsets.UTF_8.name());
    String separator = System.lineSeparator();
    assertEquals("/test (1)" + separator + " /test/path (0)" + separator + separator, standardOutput);
  }

  @Test
  public void testUpConfigLinkConfigClearZk() throws Exception {
    File tmpDir = createTempDir().toFile();

    // test upconfig
    String confsetname = "confsetone";
    final String[] upconfigArgs;
    if (random().nextBoolean()) {
      upconfigArgs = new String[] {
          "-zkhost", zkServer.getZkAddress(),
          "-cmd", ZkCLI.UPCONFIG,
          "-confdir", ExternalPaths.TECHPRODUCTS_CONFIGSET,
          "-confname", confsetname};
    } else {
      final String excluderegexOption = (random().nextBoolean() ? "--"+ZkCLI.EXCLUDE_REGEX : "-"+ZkCLI.EXCLUDE_REGEX_SHORT);
      upconfigArgs = new String[] {
          "-zkhost", zkServer.getZkAddress(),
          "-cmd", ZkCLI.UPCONFIG,
          excluderegexOption, ZkCLI.EXCLUDE_REGEX_DEFAULT,
          "-confdir", ExternalPaths.TECHPRODUCTS_CONFIGSET,
          "-confname", confsetname};
    }
    ZkCLI.main(upconfigArgs);

    assertTrue(zkClient.exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + confsetname, true));

    // print help
    // ZkCLI.main(new String[0]);

    // test linkconfig
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "linkconfig", "-collection", "collection1", "-confname", confsetname};
    ZkCLI.main(args);

    ZkNodeProps collectionProps = ZkNodeProps.load(zkClient.getData(ZkStateReader.COLLECTIONS_ZKNODE + "/collection1", null, null, true));
    assertTrue(collectionProps.containsKey("configName"));
    assertEquals(confsetname, collectionProps.getStr("configName"));

    // test down config
    File confDir = new File(tmpDir,
        "solrtest-confdropspot-" + this.getClass().getName() + "-" + System.nanoTime());
    assertFalse(confDir.exists());

    args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "downconfig", "-confdir", confDir.getAbsolutePath(), "-confname", confsetname};
    ZkCLI.main(args);

    File[] files = confDir.listFiles();
    List<String> zkFiles = zkClient.getChildren(ZkConfigManager.CONFIGS_ZKNODE + "/" + confsetname, null, true);
    assertEquals(files.length, zkFiles.size());

    File sourceConfDir = new File(ExternalPaths.TECHPRODUCTS_CONFIGSET);
    // filter out all directories starting with . (e.g. .svn)
    Collection<File> sourceFiles = FileUtils.listFiles(sourceConfDir, TrueFileFilter.INSTANCE, new RegexFileFilter("[^\\.].*"));
    for (File sourceFile :sourceFiles){
        int indexOfRelativePath = sourceFile.getAbsolutePath().lastIndexOf("sample_techproducts_configs" + File.separator + "conf");
        String relativePathofFile = sourceFile.getAbsolutePath().substring(indexOfRelativePath + 33, sourceFile.getAbsolutePath().length());
        File downloadedFile = new File(confDir,relativePathofFile);
        if (ZkConfigManager.UPLOAD_FILENAME_EXCLUDE_PATTERN.matcher(relativePathofFile).matches()) {
          assertFalse(sourceFile.getAbsolutePath() + " exists in ZK, downloaded:" + downloadedFile.getAbsolutePath(), downloadedFile.exists());
        } else {
          assertTrue(downloadedFile.getAbsolutePath() + " does not exist source:" + sourceFile.getAbsolutePath(), downloadedFile.exists());
          assertTrue(relativePathofFile+" content changed",FileUtils.contentEquals(sourceFile,downloadedFile));
        }
    }


    // test reset zk
    args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "clear", "/"};
    ZkCLI.main(args);

    assertEquals(0, zkClient.getChildren("/", null, true).size());
  }

  @Test
  public void testGet() throws Exception {
    String getNode = "/getNode";
    byte [] data = "getNode-data".getBytes(StandardCharsets.UTF_8);
    this.zkClient.create(getNode, data, CreateMode.PERSISTENT, true);
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "get", getNode};
    ZkCLI.main(args);
  }

  @Test
  public void testGetFile() throws Exception {
    File tmpDir = createTempDir().toFile();

    String getNode = "/getFileNode";
    byte [] data = "getFileNode-data".getBytes(StandardCharsets.UTF_8);
    this.zkClient.create(getNode, data, CreateMode.PERSISTENT, true);

    File file = new File(tmpDir,
        "solrtest-getfile-" + this.getClass().getName() + "-" + System.nanoTime());
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "getfile", getNode, file.getAbsolutePath()};
    ZkCLI.main(args);

    byte [] readData = FileUtils.readFileToByteArray(file);
    assertArrayEquals(data, readData);
  }

  @Test
  public void testGetFileNotExists() throws Exception {
    String getNode = "/getFileNotExistsNode";

    File file = createTempFile("newfile", null).toFile();
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "getfile", getNode, file.getAbsolutePath()};
    KeeperException e = expectThrows(KeeperException.class, () -> ZkCLI.main(args));
    assertEquals(e.code(), KeeperException.Code.NONODE);
  }

  public void testInvalidZKAddress() throws SolrException {
    SolrException ex = expectThrows(SolrException.class, () -> {
      new SolrZkClient("----------:33332", 100);
    });
    zkClient.close();
  }

  @Test
  public void testSetClusterProperty() throws Exception {
    ClusterProperties properties = new ClusterProperties(zkClient);
    // add property urlScheme=http
    String[] args = new String[]{"-zkhost", zkServer.getZkAddress(),
        "-cmd", "CLUSTERPROP", "-name", "urlScheme", "-val", "http"};
    ZkCLI.main(args);
    assertEquals("http", properties.getClusterProperty("urlScheme", "none"));

    // remove it again
    args = new String[]{"-zkhost", zkServer.getZkAddress(),
        "-cmd", "CLUSTERPROP", "-name", "urlScheme"};
    ZkCLI.main(args);
    assertNull(properties.getClusterProperty("urlScheme", (String) null));

  }

  @Test
  public void testUpdateAcls() throws Exception {
    try {
      System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
      System.setProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME, "user");
      System.setProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME, "pass");

      String[] args = new String[]{"-zkhost", zkServer.getZkAddress(), "-cmd", "updateacls", "/"};
      ZkCLI.main(args);
    } finally {
      // Need to clear these before we open the next SolrZkClient
      System.clearProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
      System.clearProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME);
      System.clearProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
    }

    boolean excepted = false;
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractDistribZkTestBase.DEFAULT_CONNECTION_TIMEOUT)) {
      zkClient.getData("/", null, null, true);
    } catch (KeeperException.NoAuthException e) {
      excepted = true;
    }
    assertTrue("Did not fail to read.", excepted);
  }

  @Override
  public void tearDown() throws Exception {
    zkClient.close();
    zkServer.shutdown();
    super.tearDown();
  }

  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStream(System.out);
    zkClient.close();
  }
}
