package org.apache.solr.cloud;

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

import java.io.File;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for ZooKeeper tests.
 */
public abstract class AbstractZooKeeperTestCase extends AbstractSolrTestCase {

  static final String ZOO_KEEPER_HOST = "localhost:2181/solr";
  static final int TIMEOUT = 10000;

  protected static Logger log = LoggerFactory
      .getLogger(AbstractZooKeeperTestCase.class);


  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  private ZooKeeperTestServer zkServer;

  public AbstractZooKeeperTestCase() {

  }

  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  public void setUp() throws Exception {
    try {
      System.setProperty("zkHost", ZOO_KEEPER_HOST);
      String zkDir = tmpDir.getAbsolutePath() + File.separator
      + "zookeeper/server1/data";
      zkServer = new ZooKeeperTestServer(zkDir);
      zkServer.run();

      buildZooKeeper(getSolrConfigFile(), getSchemaFile());

      log.info("####SETUP_START " + getName());
      dataDir = tmpDir;
      dataDir.mkdirs();

      // set some system properties for use by tests
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");

      CoreContainer.Initializer init = new CoreContainer.Initializer() {
        {
          this.dataDir = AbstractZooKeeperTestCase.this.dataDir
              .getAbsolutePath();
        }
      };

      h = new TestHarness("", init);
      lrf = h.getRequestFactory("standard", 0, 20, "version", "2.2");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    log.info("####SETUP_END " + getName());

  }

  // static to share with distrib test
  static void buildZooKeeper(String config, String schema)
      throws Exception {
    ZooKeeperWriter zkWriter = new ZooKeeperWriter(ZOO_KEEPER_HOST.substring(0, ZOO_KEEPER_HOST
        .indexOf('/')), TIMEOUT);

    zkWriter.makePath("/solr");
    zkWriter.close();

    zkWriter = new ZooKeeperWriter(ZOO_KEEPER_HOST, TIMEOUT);

    zkWriter.makePath("/collections/collection1/config=collection1");

    putConfig(zkWriter, config);
    putConfig(zkWriter, schema);
    putConfig(zkWriter, "stopwords.txt");
    putConfig(zkWriter, "protwords.txt");
    putConfig(zkWriter, "mapping-ISOLatin1Accent.txt");
    putConfig(zkWriter, "old_synonyms.txt");
    zkWriter.close();
  }

  private static void putConfig(ZooKeeperWriter zkWriter, String name) throws Exception {
    zkWriter.write("/configs/collection1/" + name, new File("solr"
        + File.separator + "conf" + File.separator + name));
  }

  public void tearDown() throws Exception {
    printLayout();
    zkServer.shutdown();
    super.tearDown();
  }

  private void printLayout() throws Exception {
    ZooKeeperReader zkReader = new ZooKeeperReader(ZOO_KEEPER_HOST.substring(0, ZOO_KEEPER_HOST
        .indexOf('/')), TIMEOUT);

    zkReader.printLayoutToStdOut();
    zkReader.close();
  }
}
