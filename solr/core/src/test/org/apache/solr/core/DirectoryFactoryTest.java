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
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DirectoryFactoryTest extends LuceneTestCase {

  public void testLockTypesUnchanged() throws Exception {
    assertEquals("simple", DirectoryFactory.LOCK_TYPE_SIMPLE);
    assertEquals("native", DirectoryFactory.LOCK_TYPE_NATIVE);
    assertEquals("single", DirectoryFactory.LOCK_TYPE_SINGLE);
    assertEquals("none", DirectoryFactory.LOCK_TYPE_NONE);
    assertEquals("hdfs", DirectoryFactory.LOCK_TYPE_HDFS);
  }

  @After
  @Before
  public void clean() {
    System.clearProperty("solr.data.home");
    System.clearProperty("solr.solr.home");
  }

  @Test
  public void testGetDataHome() throws Exception {
    MockCoreContainer cc = new MockCoreContainer("/solr/home");
    Properties cp = cc.getContainerProperties();
    boolean zkAware = cc.isZooKeeperAware();
    RAMDirectoryFactory rdf = new RAMDirectoryFactory();
    rdf.initCoreContainer(cc);
    rdf.init(new NamedList());

    // No solr.data.home property set. Absolute instanceDir
    assertDataHome("/tmp/inst1/data", "/tmp/inst1", rdf, cc);

    // Simulate solr.data.home set in solrconfig.xml <directoryFactory> tag
    NamedList args = new NamedList();
    args.add("solr.data.home", "/solrdata/");
    rdf.init(args);
    assertDataHome("/solrdata/inst_dir/data", "inst_dir", rdf, cc);
    
    // solr.data.home set with System property, and relative path
    System.setProperty("solr.data.home", "solrdata");
    rdf.init(new NamedList());
    assertDataHome("/solr/home/solrdata/inst_dir/data", "inst_dir", rdf, cc);
    // Test parsing last component of instanceDir, and using custom dataDir
    assertDataHome("/solr/home/solrdata/myinst/mydata", "/path/to/myinst", rdf, cc, "dataDir", "mydata");
  }

  private void assertDataHome(String expected, String instanceDir, RAMDirectoryFactory rdf, MockCoreContainer cc, String... properties) throws IOException {
    String dataHome = rdf.getDataHome(new CoreDescriptor("core_name", Paths.get(instanceDir), cc.containerProperties, cc.isZooKeeperAware(), properties));
    assertEquals(Paths.get(expected).toAbsolutePath(), Paths.get(dataHome).toAbsolutePath());
  }


  private static class MockCoreContainer extends CoreContainer {

    private final String mockSolrHome;

    public MockCoreContainer(String solrHome) throws IOException {
      super(new Object());
      mockSolrHome = solrHome;
      this.shardHandlerFactory = new HttpShardHandlerFactory();
      this.coreAdminHandler = new CoreAdminHandler();
    }

    @Override
    public String getSolrHome() {
      return mockSolrHome;
    }
  }

}
