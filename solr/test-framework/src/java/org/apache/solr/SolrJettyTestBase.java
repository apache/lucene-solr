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
package org.apache.solr;

import java.io.File;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.util.ExternalPaths;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class SolrJettyTestBase extends SolrTestCaseJ4
{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static Set<JettySolrRunner> jettys = ConcurrentHashMap.newKeySet();
  public static Set<SolrClient> clients = ConcurrentHashMap.newKeySet();

  protected static volatile SolrClient client;
  protected static volatile JettySolrRunner jetty;

  public static int port;
  public static String context;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    if (jetty == null && jettys.size() > 0) {
      jetty = jettys.iterator().next();
    }

    if (client == null && jetty != null) {
      SolrClient newClient = createNewSolrClient(jetty);
      clients.add(newClient);
      client = newClient;
    }

  }

  @AfterClass
  public static void afterSolrJettyTestBase() throws Exception {
    for (SolrClient client : clients) {
      IOUtils.closeQuietly(client);
    }
    clients.clear();

    for (JettySolrRunner jetty : jettys) {
      jetty.stop();
    }
    jettys.clear();

    jetty = null;
    client = null;
    port = 0;
    context = null;
  }

  public static JettySolrRunner createAndStartJetty(String solrHome, String configFile, String schemaFile, String context,
                                            boolean stopAtShutdown, SortedMap<ServletHolder,String> extraServlets)
      throws Exception {
    // creates the data dir

    initCore(null, null, solrHome);

    context = context==null ? "/solr" : context;
    SolrJettyTestBase.context = context;

    JettyConfig jettyConfig = JettyConfig.builder()
        .setContext(context)
        .stopAtShutdown(stopAtShutdown)
        .withServlets(extraServlets)
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();

    Properties nodeProps = new Properties();
    if (configFile != null)
      nodeProps.setProperty("solrconfig", configFile);
    if (schemaFile != null)
      nodeProps.setProperty("schema", schemaFile);
    if (System.getProperty("solr.data.dir") == null && System.getProperty("solr.hdfs.home") == null) {
      nodeProps.setProperty("solr.data.dir", SolrTestUtil.createTempDir().toFile().getCanonicalPath());
    }

    return createAndStartJetty(solrHome, nodeProps, jettyConfig);
  }

  public static JettySolrRunner createAndStartJetty(String solrHome, String configFile, String context) throws Exception {
    return createAndStartJetty(solrHome, configFile, null, context, true, null);
  }

  public static JettySolrRunner createAndStartJetty(String solrHome, JettyConfig jettyConfig) throws Exception {
    return createAndStartJetty(solrHome, new Properties(), jettyConfig);
  }

  public static JettySolrRunner createAndStartJetty(String solrHome) throws Exception {
    return createAndStartJetty(solrHome, new Properties(), JettyConfig.builder().withSSLConfig(sslConfig.buildServerSSLConfig()).build());
  }

  public static JettySolrRunner createAndStartJetty(String solrHome, Properties nodeProperties, JettyConfig jettyConfig) throws Exception {


    Path coresDir = SolrTestUtil.createTempDir().resolve("cores");

    Properties props = new Properties();
    props.setProperty("name", DEFAULT_TEST_CORENAME);
    props.setProperty("configSet", "collection1");
    props.setProperty("config", "${solrconfig:solrconfig.xml}");
    props.setProperty("schema", "${schema:schema.xml}");

    writeCoreProperties(coresDir.resolve("core"), props, "RestTestBase");

    Properties nodeProps = new Properties(nodeProperties);
    nodeProps.setProperty("coreRootDirectory", coresDir.toString());
    nodeProps.setProperty("configSetBaseDir", solrHome);

    JettySolrRunner jetty = new JettySolrRunner(solrHome, nodeProps,
        jettyConfig);
    jetty.start();
    port = jetty.getLocalPort();
    log.info("Jetty Assigned Port#{}", port);

    jettys.add(jetty);

    return jetty;
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public synchronized SolrClient getSolrClient(JettySolrRunner jetty) {

   return client;
  }

  /**
   * Create a new solr client.
   * If createJetty was called, an http implementation will be created,
   * otherwise an embedded implementation will be created.
   * Subclasses should override for other options.
   */
  public SolrClient createNewSolrClient(JettySolrRunner jetty) {
    try {
      // setup the client...
      final String url = jetty.getBaseUrl() + "/" + "collection1";
      final Http2SolrClient client = getHttpSolrClient(url, DEFAULT_CONNECTION_TIMEOUT);
      return client;
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  // Sets up the necessary config files for Jetty. At least some tests require that the solrconfig from the test
  // file directory are used, but some also require that the solr.xml file be explicitly there as of SOLR-4817
  public static void setupJettyTestHome(File solrHome, String collection) throws Exception {
    copySolrHomeToTemp(solrHome, collection);
  }

  public static void cleanUpJettyHome(File solrHome) throws Exception {
    if (solrHome.exists()) {
      FileUtils.deleteDirectory(solrHome);
    }
  }

  public static String legacyExampleCollection1SolrHome() {
    String sourceHome = ExternalPaths.SOURCE_HOME;
    if (sourceHome == null)
      throw new IllegalStateException("No source home! Cannot create the legacy example solr home directory.");

    String legacyExampleSolrHome = null;
    try {
      File tempSolrHome = LuceneTestCase.createTempDir().toFile();
      org.apache.commons.io.FileUtils.copyFileToDirectory(new File(sourceHome, "server/solr/solr.xml"), tempSolrHome);
      File collection1Dir = new File(tempSolrHome, "collection1");
      org.apache.commons.io.FileUtils.forceMkdir(collection1Dir);

      File configSetDir = new File(sourceHome, "server/solr/configsets/sample_techproducts_configs/conf");
      org.apache.commons.io.FileUtils.copyDirectoryToDirectory(configSetDir, collection1Dir);
      Properties props = new Properties();
      props.setProperty("name", "collection1");
      OutputStreamWriter writer = null;
      try {
        writer = new OutputStreamWriter(FileUtils.openOutputStream(
            new File(collection1Dir, "core.properties")), StandardCharsets.UTF_8);
        props.store(writer, null);
      } finally {
        if (writer != null) {
          try {
            writer.close();
          } catch (Exception ignore){
            ParWork.propagateInterrupt(ignore);
          }
        }
      }
      legacyExampleSolrHome = tempSolrHome.getAbsolutePath();
    } catch (Exception exc) {
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    return legacyExampleSolrHome;
  }

}
