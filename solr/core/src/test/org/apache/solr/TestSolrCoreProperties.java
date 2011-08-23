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
package org.apache.solr;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.Properties;


/**
 * <p> Test for Loading core properties from a properties file </p>
 *
 *
 * @since solr 1.4
 */
public class TestSolrCoreProperties extends LuceneTestCase {
  private static final String CONF_DIR = "." + File.separator + "solr" + File.separator + "conf" + File.separator;
  JettySolrRunner solrJetty;
  SolrServer client;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setUpMe();
    System.setProperty("solr.solr.home", getHomeDir());
    System.setProperty("solr.data.dir", getDataDir());
    
    solrJetty = new JettySolrRunner("/solr", 0);

    solrJetty.start();
    String url = "http://localhost:" + solrJetty.getLocalPort() + "/solr";
    client = new CommonsHttpSolrServer(url);

  }

  @Override
  public void tearDown() throws Exception {
    solrJetty.stop();
    AbstractSolrTestCase.recurseDelete(homeDir);
    super.tearDown();
  }

  public void testSimple() throws SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    QueryResponse res = client.query(params);
    assertEquals(0, res.getResults().getNumFound());
  }


  File homeDir;
  File confDir;
  File dataDir;

  /**
   * if masterPort is null, this instance is a master -- otherwise this instance is a slave, and assumes the master is
   * on localhost at the specified port.
   */


  public String getHomeDir() {
    return homeDir.toString();
  }

  public String getSchemaFile() {
    return CONF_DIR + "schema-replication1.xml";
  }

  public String getConfDir() {
    return confDir.toString();
  }

  public String getDataDir() {
    return dataDir.toString();
  }

  public String getSolrConfigFile() {
    return CONF_DIR + "solrconfig-solcoreproperties.xml";
  }

  public void setUpMe() throws Exception {

    homeDir = new File(TEMP_DIR,
            getClass().getName() + "-" + System.currentTimeMillis());


    dataDir = new File(homeDir, "data");
    confDir = new File(homeDir, "conf");


    homeDir.mkdirs();
    dataDir.mkdirs();
    confDir.mkdirs();

    File f = new File(confDir, "solrconfig.xml");
    copyFile(SolrTestCaseJ4.getFile(getSolrConfigFile()), f);

    f = new File(confDir, "schema.xml");
    copyFile(SolrTestCaseJ4.getFile(getSchemaFile()), f);
    Properties p = new Properties();
    p.setProperty("foo.foo1", "f1");
    p.setProperty("foo.foo2", "f2");
    FileOutputStream fos = new FileOutputStream(confDir + File.separator + "solrcore.properties");
    p.store(fos, null);
    fos.close();
    IOUtils.closeQuietly(fos);

  }


  private void copyFile(File src, File dst) throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(src));
    Writer out = new FileWriter(dst);

    for (String line = in.readLine(); null != line; line = in.readLine()) {
      out.write(line);
    }
    in.close();
    out.close();
  }
}
