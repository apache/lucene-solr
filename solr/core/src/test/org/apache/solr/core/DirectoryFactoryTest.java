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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DirectoryFactoryTest extends SolrTestCase {

  private static Path solrHome = null;
  private static SolrResourceLoader loader = null;

  @BeforeClass
  public static void setupLoader() throws Exception {
    solrHome = Paths.get(createTempDir().toAbsolutePath().toString());
    loader = new SolrResourceLoader(solrHome);
  }

  @AfterClass
  public static void cleanupLoader() throws Exception {
    if (loader != null) {
      loader.close();
    }
    loader = null;
  }

  @After
  @Before
  public void clean() {
    System.clearProperty("solr.data.home");
    System.clearProperty("solr.solr.home");
    System.clearProperty("test.solr.data.home");
  }

  @Test
  public void testLockTypesUnchanged() throws Exception {
    assertEquals("simple", DirectoryFactory.LOCK_TYPE_SIMPLE);
    assertEquals("native", DirectoryFactory.LOCK_TYPE_NATIVE);
    assertEquals("single", DirectoryFactory.LOCK_TYPE_SINGLE);
    assertEquals("none", DirectoryFactory.LOCK_TYPE_NONE);
    assertEquals("hdfs", DirectoryFactory.LOCK_TYPE_HDFS);
  }

  @Test
  public void testGetDataHomeRAMDirectory() throws Exception {
    doTestGetDataHome(RAMDirectoryFactory.class);
  }
  
  @Test
  public void testGetDataHomeByteBuffersDirectory() throws Exception {
    doTestGetDataHome(ByteBuffersDirectoryFactory.class);
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void doTestGetDataHome(Class<? extends DirectoryFactory> directoryFactoryClass) throws Exception {
    NodeConfig config = loadNodeConfig("/solr/solr-solrDataHome.xml");
    CoreContainer cc = new CoreContainer(config);
    Properties cp = cc.getContainerProperties();
    DirectoryFactory df = directoryFactoryClass.newInstance();
    df.initCoreContainer(cc);
    df.init(new NamedList());

    // No solr.data.home property set. Absolute instanceDir
    assertDataHome("/tmp/inst1/data", "/tmp/inst1", df, cc);

    // Simulate solr.data.home set in solrconfig.xml <directoryFactory> tag
    NamedList args = new NamedList();
    args.add("solr.data.home", "/solrdata/");
    df.init(args);
    assertDataHome("/solrdata/inst_dir/data", "inst_dir", df, cc);
    
    // solr.data.home set with System property, and relative path
    System.setProperty("solr.data.home", "solrdata");
    config = loadNodeConfig("/solr/solr-solrDataHome.xml");
    cc = new CoreContainer(config);
    df = directoryFactoryClass.newInstance();
    df.initCoreContainer(cc);
    df.init(new NamedList());
    assertDataHome(solrHome.resolve("solrdata/inst_dir/data").toAbsolutePath().toString(), "inst_dir", df, cc);
    // Test parsing last component of instanceDir, and using custom dataDir
    assertDataHome(solrHome.resolve("solrdata/myinst/mydata").toAbsolutePath().toString(), "/path/to/myinst", df, cc, "dataDir", "mydata");
    // solr.data.home set but also solrDataHome set in solr.xml, which should override the former
    System.setProperty("test.solr.data.home", "/foo");
    config = loadNodeConfig("/solr/solr-solrDataHome.xml");
    cc = new CoreContainer(config);
    df = directoryFactoryClass.newInstance();
    df.initCoreContainer(cc);
    df.init(new NamedList());
    assertDataHome("/foo/inst_dir/data", "inst_dir", df, cc);
  }

  private void assertDataHome(String expected, String instanceDir, DirectoryFactory df, CoreContainer cc, String... properties) throws IOException {
    String dataHome = df.getDataHome(new CoreDescriptor("core_name", Paths.get(instanceDir), cc, properties));
    assertEquals(Paths.get(expected).toAbsolutePath(), Paths.get(dataHome).toAbsolutePath());
  }


  private NodeConfig loadNodeConfig(String config) throws Exception {
    InputStream is = DirectoryFactoryTest.class.getResourceAsStream(config);
    return SolrXmlConfig.fromInputStream(solrHome, is, new Properties());
  }
}
