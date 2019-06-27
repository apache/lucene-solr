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
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.util.SolrCLI;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrCLIZkUtilsTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    zkAddr = cluster.getZkServer().getZkAddress();
    zkClient = new SolrZkClient(zkAddr, 30000);

  }

  @AfterClass
  public static void closeConn() {
    if (null != zkClient) {
      zkClient.close();
      zkClient = null;
    }
    zkAddr = null;
  }

  private static String zkAddr;
  private static SolrZkClient zkClient;

  @Test
  public void testUpconfig() throws Exception {
    // Use a full, explicit path for configset.

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");
    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "upconfig1", zkAddr);
    // Now do we have that config up on ZK?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/upconfig1");

    // Now just use a name in the configsets directory, do we find it?
    configSet = TEST_PATH().resolve("configsets");

    String[] args = new String[]{
        "-confname", "upconfig2",
        "-confdir", "cloud-subdirs",
        "-zkHost", zkAddr,
        "-configsetsDir", configSet.toAbsolutePath().toString(),
    };

    SolrCLI.ConfigSetUploadTool tool = new SolrCLI.ConfigSetUploadTool();

    int res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    assertEquals("tool should have returned 0 for success ", 0, res);
    // Now do we have that config up on ZK?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/upconfig2");

    // do we barf on a bogus path?
    args = new String[]{
        "-confname", "upconfig3",
        "-confdir", "nothinghere",
        "-zkHost", zkAddr,
        "-configsetsDir", configSet.toAbsolutePath().toString(),
    };

    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    assertTrue("tool should have returned non-zero for failure ", 0 != res);

    String content = new String(zkClient.getData("/configs/upconfig2/schema.xml", null, null, true), StandardCharsets.UTF_8);
    assertTrue("There should be content in the node! ", content.contains("Apache Software Foundation"));

  }

  @Test
  public void testDownconfig() throws Exception {
    Path tmp = Paths.get(createTempDir("downConfigNewPlace").toAbsolutePath().toString(), "myconfset");

    // First we need a configset on ZK to bring down. 
    
    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");
    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "downconfig1", zkAddr);
    // Now do we have that config up on ZK?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/downconfig1");

    String[] args = new String[]{
        "-confname", "downconfig1",
        "-confdir", tmp.toAbsolutePath().toString(),
        "-zkHost", zkAddr,
    };

    SolrCLI.ConfigSetDownloadTool downTool = new SolrCLI.ConfigSetDownloadTool();
    int res = downTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(downTool.getOptions()), args));
    assertEquals("Download should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(Paths.get(tmp.toAbsolutePath().toString(), "conf"), "/configs/downconfig1");

    // Insure that empty files don't become directories (SOLR-11198)

    Path emptyFile = Paths.get(tmp.toAbsolutePath().toString(), "conf", "stopwords", "emptyfile");
    Files.createFile(emptyFile);

    // Now copy it up and back and insure it's still a file in the new place
    AbstractDistribZkTestBase.copyConfigUp(tmp.getParent(), "myconfset", "downconfig2", zkAddr);
    Path tmp2 = createTempDir("downConfigNewPlace2");
    downTool = new SolrCLI.ConfigSetDownloadTool();
    args = new String[]{
        "-confname", "downconfig2",
        "-confdir", tmp2.toAbsolutePath().toString(),
        "-zkHost", zkAddr,
    };

    res = downTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(downTool.getOptions()), args));
    assertEquals("Download should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(Paths.get(tmp.toAbsolutePath().toString(), "conf"), "/configs/downconfig2");
    // And insure the empty file is a text file
    Path destEmpty = Paths.get(tmp2.toAbsolutePath().toString(), "conf", "stopwords", "emptyfile");
    assertTrue("Empty files should NOT be copied down as directories", destEmpty.toFile().isFile());

  }

  @Test
  public void testCp() throws Exception {
    // First get something up on ZK

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "cp1", zkAddr);

    // Now copy it somewhere else on ZK.
    String[] args = new String[]{
        "-src", "zk:/configs/cp1",
        "-dst", "zk:/cp2",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    SolrCLI.ZkCpTool cpTool = new SolrCLI.ZkCpTool();

    int res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy from zk -> zk should have succeeded.", 0, res);
    verifyZnodesMatch("/configs/cp1", "/cp2");


    // try with zk->local
    Path tmp = createTempDir("tmpNewPlace2");
    args = new String[]{
        "-src", "zk:/configs/cp1",
        "-dst", "file:" + tmp.toAbsolutePath().toString(),
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(tmp, "/configs/cp1");


    // try with zk->local  no file: prefix
    tmp = createTempDir("tmpNewPlace3");
    args = new String[]{
        "-src", "zk:/configs/cp1",
        "-dst", tmp.toAbsolutePath().toString(),
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(tmp, "/configs/cp1");


    // try with local->zk
    args = new String[]{
        "-src", srcPathCheck.toAbsolutePath().toString(),
        "-dst", "zk:/cp3",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(srcPathCheck, "/cp3");

    // try with local->zk, file: specified
    args = new String[]{
        "-src", "file:" + srcPathCheck.toAbsolutePath().toString(),
        "-dst", "zk:/cp4",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(srcPathCheck, "/cp4");

    // try with recurse not specified
    args = new String[]{
        "-src", "file:" + srcPathCheck.toAbsolutePath().toString(),
        "-dst", "zk:/cp5Fail",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertTrue("Copy should NOT have succeeded, recurse not specified.", 0 != res);

    // try with recurse = false
    args = new String[]{
        "-src", "file:" + srcPathCheck.toAbsolutePath().toString(),
        "-dst", "zk:/cp6Fail",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertTrue("Copy should NOT have succeeded, recurse set to false.", 0 != res);


    // NOTE: really can't test copying to '.' because the test framework doesn't allow altering the source tree
    // and at least IntelliJ's CWD is in the source tree.

    // copy to local ending in separator
    //src and cp3 and cp4 are valid
    String localSlash = tmp.normalize() +  File.separator +"cpToLocal" + File.separator;
    args = new String[]{
        "-src", "zk:/cp3/schema.xml",
        "-dst", localSlash,
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should nave created intermediate directory locally.", 0, res);
    assertTrue("File should have been copied to a directory successfully", Files.exists(Paths.get(localSlash, "schema.xml")));

    // copy to ZK ending in '/'.
    //src and cp3 are valid
    args = new String[]{
        "-src", "file:" + srcPathCheck.normalize().toAbsolutePath().toString() + File.separator + "solrconfig.xml",
        "-dst", "zk:/powerup/",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy up to intermediate file should have succeeded.", 0, res);
    assertTrue("Should have created an intermediate node on ZK", zkClient.exists("/powerup/solrconfig.xml", true));

    // copy individual file up
    //src and cp3 are valid
    args = new String[]{
        "-src", "file:" + srcPathCheck.normalize().toAbsolutePath().toString() + File.separator + "solrconfig.xml",
        "-dst", "zk:/copyUpFile.xml",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy up to named file should have succeeded.", 0, res);
    assertTrue("Should NOT have created an intermediate node on ZK", zkClient.exists("/copyUpFile.xml", true));

    // copy individual file down
    //src and cp3 are valid

    String localNamed = tmp.normalize().toString() + File.separator + "localnamed" + File.separator +  "renamed.txt";
    args = new String[]{
        "-src", "zk:/cp4/solrconfig.xml",
        "-dst", "file:" + localNamed,
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy to local named file should have succeeded.", 0, res);
    Path locPath = Paths.get(localNamed);
    assertTrue("Should have found file: " + localNamed, Files.exists(locPath));
    assertTrue("Should be an individual file", Files.isRegularFile(locPath));
    assertTrue("File should have some data", Files.size(locPath) > 100);
    boolean foundApache = false;
    for (String line : Files.readAllLines(locPath, Charset.forName("UTF-8"))) {
      if (line.contains("Apache Software Foundation")) {
        foundApache = true;
        break;
      }
    }
    assertTrue("Should have found Apache Software Foundation in the file! ", foundApache);


    // Test copy from somwehere in ZK to the root of ZK.
    args = new String[]{
        "-src", "zk:/cp4/solrconfig.xml",
        "-dst", "zk:/",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy from somewhere in ZK to ZK root should have succeeded.", 0, res);
    assertTrue("Should have found znode /solrconfig.xml: ", zkClient.exists("/solrconfig.xml", true));

    // Check that the form path/ works for copying files up. Should append the last bit of the source path to the dst
    args = new String[]{
        "-src", "file:" + srcPathCheck.toAbsolutePath().toString(),
        "-dst", "zk:/cp7/",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(srcPathCheck, "/cp7/" + srcPathCheck.getFileName().toString());

    // Check for an intermediate ZNODE having content. You know cp7/stopwords is a parent node.
    tmp = createTempDir("dirdata");
    Path file = Paths.get(tmp.toAbsolutePath().toString(), "zknode.data");
    List<String> lines = new ArrayList<>();
    lines.add("{Some Arbitrary Data}");
    Files.write(file, lines, Charset.forName("UTF-8"));
    // First, just copy the data up the cp7 since it's a directory.
    args = new String[]{
        "-src", "file:" + file.toAbsolutePath().toString(),
        "-dst", "zk:/cp7/conf/stopwords/",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    String content = new String(zkClient.getData("/cp7/conf/stopwords", null, null, true), StandardCharsets.UTF_8);
    assertTrue("There should be content in the node! ", content.contains("{Some Arbitrary Data}"));


    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    tmp = createTempDir("cp8");
    args = new String[]{
        "-src", "zk:/cp7",
        "-dst", "file:" + tmp.toAbsolutePath().toString(),
        "-recurse", "true",
        "-zkHost", zkAddr,
    };
    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    // Next, copy cp7 down and verify that zknode.data exists for cp7
    Path zData = Paths.get(tmp.toAbsolutePath().toString(), "conf/stopwords/zknode.data");
    assertTrue("znode.data should have been copied down", zData.toFile().exists());

    // Finally, copy up to cp8 and verify that the data is up there.
    args = new String[]{
        "-src", "file:" + tmp.toAbsolutePath().toString(),
        "-dst", "zk:/cp9",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    content = new String(zkClient.getData("/cp9/conf/stopwords", null, null, true), StandardCharsets.UTF_8);
    assertTrue("There should be content in the node! ", content.contains("{Some Arbitrary Data}"));

    // Copy an individual empty file up and back down and insure it's still a file
    Path emptyFile = Paths.get(tmp.toAbsolutePath().toString(), "conf", "stopwords", "emptyfile");
    Files.createFile(emptyFile);

    args = new String[]{
        "-src", "file:" + emptyFile.toAbsolutePath().toString(),
        "-dst", "zk:/cp7/conf/stopwords/emptyfile",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    Path tmp2 = createTempDir("cp9");
    Path emptyDest = Paths.get(tmp2.toAbsolutePath().toString(), "emptyfile");
    args = new String[]{
        "-src", "zk:/cp7/conf/stopwords/emptyfile",
        "-dst", "file:" + emptyDest.toAbsolutePath().toString(),
        "-recurse", "false",
        "-zkHost", zkAddr,
    };
    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    assertTrue("Empty files should NOT be copied down as directories", emptyDest.toFile().isFile());

    // Now with recursive copy

    args = new String[]{
        "-src", "file:" + emptyFile.getParent().getParent().toString(),
        "-dst", "zk:/cp10",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    // Now copy it all back and make sure empty file is still a file when recursively copying.
    tmp2 = createTempDir("cp10");
    args = new String[]{
        "-src", "zk:/cp10",
        "-dst", "file:" + tmp2.toAbsolutePath().toString(),
        "-recurse", "true",
        "-zkHost", zkAddr,
    };
    res = cpTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(cpTool.getOptions()), args));
    assertEquals("Copy should have succeeded.", 0, res);

    Path locEmpty = Paths.get(tmp2.toAbsolutePath().toString(), "stopwords", "emptyfile");
    assertTrue("Empty files should NOT be copied down as directories", locEmpty.toFile().isFile());
  }

  @Test
  public void testMv() throws Exception {

    // First get something up on ZK

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "mv1", zkAddr);

    // Now move it somewhere else.
    String[] args = new String[]{
        "-src", "zk:/configs/mv1",
        "-dst", "zk:/mv2",
        "-zkHost", zkAddr,
    };

    SolrCLI.ZkMvTool mvTool = new SolrCLI.ZkMvTool();

    int res = mvTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(mvTool.getOptions()), args));
    assertEquals("Move should have succeeded.", 0, res);

    // Now does the moved directory match the original on disk?
    verifyZkLocalPathsMatch(srcPathCheck, "/mv2");
    // And are we sure the old path is gone?
    assertFalse("/configs/mv1 Znode should not be there: ", zkClient.exists("/configs/mv1", true));

    // Files are in mv2
    // Now fail if we specify "file:". Everything should still be in /mv2
    args = new String[]{
        "-src", "file:" + File.separator + "mv2",
        "-dst", "/mv3",
        "-zkHost", zkAddr,
    };

    // Still in mv2
    res = mvTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(mvTool.getOptions()), args));
    assertTrue("Move should NOT have succeeded with file: specified.", 0 != res);

    // Let's move it to yet another place with no zk: prefix.
    args = new String[]{
        "-src", "/mv2",
        "-dst", "/mv4",
        "-zkHost", zkAddr,
    };

    res = mvTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(mvTool.getOptions()), args));
    assertEquals("Move should have succeeded.", 0, res);

    assertFalse("Znode /mv3 really should be gone", zkClient.exists("/mv3", true));

    // Now does the moved directory match the original on disk?
    verifyZkLocalPathsMatch(srcPathCheck, "/mv4");

    args = new String[]{
        "-src", "/mv4/solrconfig.xml",
        "-dst", "/testmvsingle/solrconfig.xml",
        "-zkHost", zkAddr,
    };

    res = mvTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(mvTool.getOptions()), args));
    assertEquals("Move should have succeeded.", 0, res);
    assertTrue("Should be able to move a single file", zkClient.exists("/testmvsingle/solrconfig.xml", true));

    zkClient.makePath("/parentNode", true);

    // what happens if the destination ends with a slash?
    args = new String[]{
        "-src", "/mv4/schema.xml",
        "-dst", "/parentnode/",
        "-zkHost", zkAddr,
    };

    res = mvTool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(mvTool.getOptions()), args));
    assertEquals("Move should have succeeded.", 0, res);
    assertTrue("Should be able to move a single file to a parent znode", zkClient.exists("/parentnode/schema.xml", true));
    String content = new String(zkClient.getData("/parentnode/schema.xml", null, null, true), StandardCharsets.UTF_8);
    assertTrue("There should be content in the node! ", content.contains("Apache Software Foundation"));
  }

  @Test
  public void testLs() throws Exception {

    Path configSet = TEST_PATH().resolve("configsets");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "lister", zkAddr);

    // Should only find a single level.
    String[] args = new String[]{
        "-path", "/configs",
        "-zkHost", zkAddr,
    };


    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos, false, StandardCharsets.UTF_8.name());
    SolrCLI.ZkLsTool tool = new SolrCLI.ZkLsTool(ps);


    int res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    String content = new String(baos.toByteArray(), StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertFalse("Return should NOT contain a child node", content.contains("solrconfig.xml"));


    // simple ls recurse=false
    args = new String[]{
        "-path", "/configs",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };


    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    content = new String(baos.toByteArray(), StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertFalse("Return should NOT contain a child node", content.contains("solrconfig.xml"));

    // recurse=true
    args = new String[]{
        "-path", "/configs",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };


    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    content = new String(baos.toByteArray(), StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertTrue("Return should contain a child node", content.contains("solrconfig.xml"));

    // Saw a case where going from root foo'd, so test it.
    args = new String[]{
        "-path", "/",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };


    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    content = new String(baos.toByteArray(), StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertTrue("Return should contain a child node", content.contains("solrconfig.xml"));

    args = new String[]{
        "-path", "/",
        "-zkHost", zkAddr,
    };
    
    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    content = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertEquals("List should have succeeded", res, 0);
    assertFalse("Return should not contain /zookeeper", content.contains("/zookeeper"));

    // Saw a case where ending in slash foo'd, so test it.
    args = new String[]{
        "-path", "/configs/",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    content = new String(baos.toByteArray(), StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertTrue("Return should contain a child node", content.contains("solrconfig.xml"));

  }

  @Test
  public void testRm() throws Exception {
    
    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "rm1", zkAddr);
    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "rm2", zkAddr);

    // Should fail if recurse not set.
    String[] args = new String[]{
        "-path", "/configs/rm1",
        "-zkHost", zkAddr,
    };

    SolrCLI.ZkRmTool tool = new SolrCLI.ZkRmTool();

    int res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));

    assertTrue("Should have failed to remove node with children unless -recurse is set to true", res != 0);

    // Are we sure all the znodes are still there?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/rm1");

    args = new String[]{
        "-path", "zk:/configs/rm1",
        "-recurse", "false",
        "-zkHost", zkAddr,
    };

    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));

    assertTrue("Should have failed to remove node with children if -recurse is set to false", res != 0);

    args = new String[]{
        "-path", "/configs/rm1",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    assertEquals("Should have removed node /configs/rm1", res, 0);
    assertFalse("Znode /configs/toremove really should be gone", zkClient.exists("/configs/rm1", true));

    // Check that zk prefix also works.
    args = new String[]{
        "-path", "zk:/configs/rm2",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    
    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    assertEquals("Should have removed node /configs/rm2", res, 0);
    assertFalse("Znode /configs/toremove2 really should be gone", zkClient.exists("/configs/rm2", true));
    
    // This should silently just refuse to do anything to the / or /zookeeper
    args = new String[]{
        "-path", "zk:/",
        "-recurse", "true",
        "-zkHost", zkAddr,
    };

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "rm3", zkAddr);
    res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    assertFalse("Should fail when trying to remove /.", res == 0);
  }

  // Check that all children of fileRoot are children of zkRoot and vice-versa
  private void verifyZkLocalPathsMatch(Path fileRoot, String zkRoot) throws IOException, KeeperException, InterruptedException {
    verifyAllFilesAreZNodes(fileRoot, zkRoot);
    verifyAllZNodesAreFiles(fileRoot, zkRoot);
  }

  private static boolean isEphemeral(String zkPath) throws KeeperException, InterruptedException {
    Stat znodeStat = zkClient.exists(zkPath, null, true);
    return znodeStat.getEphemeralOwner() != 0;
  }

  void verifyAllZNodesAreFiles(Path fileRoot, String zkRoot) throws KeeperException, InterruptedException {

    for (String child : zkClient.getChildren(zkRoot, null, true)) {
      // Skip ephemeral nodes
      if (zkRoot.endsWith("/") == false) zkRoot += "/";
      if (isEphemeral(zkRoot + child)) continue;
      
      Path thisPath = Paths.get(fileRoot.toAbsolutePath().toString(), child);
      assertTrue("Znode " + child + " should have been found on disk at " + fileRoot.toAbsolutePath().toString(),
          Files.exists(thisPath));
      verifyAllZNodesAreFiles(thisPath, zkRoot + child);
    }
  }

  void verifyAllFilesAreZNodes(Path fileRoot, String zkRoot) throws IOException {
    Files.walkFileTree(fileRoot, new SimpleFileVisitor<Path>() {
      void checkPathOnZk(Path path) {
        String znode = ZkMaintenanceUtils.createZkNodeName(zkRoot, fileRoot, path);
        try { // It's easier to catch this exception and fail than catch it everywher eles.
          assertTrue("Should have found " + znode + " on Zookeeper", zkClient.exists(znode, true));
        } catch (Exception e) {
          fail("Caught unexpected exception " + e.getMessage() + " Znode we were checking " + znode);
        }
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        assertTrue("Path should start at proper place!", file.startsWith(fileRoot));
        checkPathOnZk(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

        checkPathOnZk(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  // Insure that all znodes in first are in second and vice-versa
  private void verifyZnodesMatch(String first, String second) throws KeeperException, InterruptedException {
    verifyFirstZNodesInSecond(first, second);
    verifyFirstZNodesInSecond(second, first);
  }

  // Note, no folderol here with Windows path names. 
  private void verifyFirstZNodesInSecond(String first, String second) throws KeeperException, InterruptedException {
    for (String node : zkClient.getChildren(first, null, true)) {
      String fNode = first + "/" + node;
      String sNode = second + "/" + node;
      assertTrue("Node " + sNode + " not found. Exists on " + fNode, zkClient.exists(sNode, true));
      verifyFirstZNodesInSecond(fNode, sNode);
    }
  }
}
