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

package org.apache.solr.morphlines.solr;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Collector;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.stdlib.PipeBuilder;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;

public abstract class AbstractSolrMorphlineZkTestBase extends AbstractFullDistribZkTestBase {
  private static final File solrHomeDirectory = new File(TEMP_DIR, AbstractSolrMorphlineZkTestBase.class.getName());
  
  protected static final String RESOURCES_DIR = ExternalPaths.SOURCE_HOME + "/contrib/map-reduce/src/test-files";  
  private static final File SOLR_INSTANCE_DIR = new File(RESOURCES_DIR + "/solr");
  private static final File SOLR_CONF_DIR = new File(RESOURCES_DIR + "/solr/collection1");

  protected Collector collector;
  protected Command morphline;

  @Override
  public String getSolrHome() {
    return solrHomeDirectory.getPath();
  }
  
  public AbstractSolrMorphlineZkTestBase() {
    fixShardCount = true;
    sliceCount = 3;
    shardCount = 3;
  }
  
  @BeforeClass
  public static void setupClass() throws Exception {
    AbstractZkTestCase.SOLRHOME = solrHomeDirectory;
    FileUtils.copyDirectory(SOLR_INSTANCE_DIR, solrHomeDirectory);
    createTempDir();
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("host", "127.0.0.1");
    System.setProperty("numShards", Integer.toString(sliceCount));
    uploadConfFiles();
    collector = new Collector();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("host");
    System.clearProperty("numShards");
  }
  
  @Test
  @Override
  public void testDistribSearch() throws Exception {
    super.testDistribSearch();
  }
  
  @Override
  protected void commit() throws Exception {
    Notifications.notifyCommitTransaction(morphline);    
    super.commit();
  }
  
  protected Command parse(String file) throws IOException {
    return parse(file, "collection1");
  }
  
  protected Command parse(String file, String collection) throws IOException {
    SolrLocator locator = new SolrLocator(createMorphlineContext());
    locator.setCollectionName(collection);
    locator.setZkHost(zkServer.getZkAddress());
    //locator.setServerUrl(cloudJettys.get(0).url); // TODO: download IndexSchema from solrUrl not yet implemented
    //locator.setSolrHomeDir(SOLR_HOME_DIR.getPath());
    Config config = new Compiler().parse(new File(RESOURCES_DIR + "/" + file + ".conf"), locator.toConfig("SOLR_LOCATOR"));
    config = config.getConfigList("morphlines").get(0);
    return createMorphline(config);
  }
  
  private Command createMorphline(Config config) {
    return new PipeBuilder().build(config, null, collector, createMorphlineContext());
  }

  private MorphlineContext createMorphlineContext() {
    return new MorphlineContext.Builder()
      .setExceptionHandler(new FaultTolerance(false, false, SolrServerException.class.getName()))
      .setMetricRegistry(new MetricRegistry())
      .build();
  }
  
  protected void startSession() {
    Notifications.notifyStartSession(morphline);
  }

  protected ListMultimap<String, Object> next(Iterator<SolrDocument> iter) {
    SolrDocument doc = iter.next();
    Record record = toRecord(doc);
    record.removeAll("_version_"); // the values of this field are unknown and internal to solr
    return record.getFields();    
  }
  
  private Record toRecord(SolrDocument doc) {
    Record record = new Record();
    for (String key : doc.keySet()) {
      record.getFields().replaceValues(key, doc.getFieldValues(key));        
    }
    return record;
  }
  
  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(solrHome.getAbsolutePath(),
        context, 0, solrConfigOverride, schemaOverride, true, null, sslConfig);

    jetty.setShards(shardList);
    
    if (System.getProperty("collection") == null) {
      System.setProperty("collection", "collection1");
    }
    
    jetty.start();
    
    System.clearProperty("collection");
    
    return jetty;
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String name) throws Exception {
    putConfig(zkClient, solrhome, name, name);
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String srcName, String destName)
      throws Exception {
    
    File file = new File(solrhome, "conf" + File.separator + srcName);
    if (!file.exists()) {
      // LOG.info("skipping " + file.getAbsolutePath() +
      // " because it doesn't exist");
      return;
    }
    
    String destPath = "/configs/conf1/" + destName;
    // LOG.info("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
  }
  
  private void uploadConfFiles() throws Exception {
    // upload our own config files
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000);
    putConfig(zkClient, SOLR_CONF_DIR, "solrconfig.xml");
    putConfig(zkClient, SOLR_CONF_DIR, "schema.xml");
    putConfig(zkClient, SOLR_CONF_DIR, "elevate.xml");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_en.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_ar.txt");
    
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_bg.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_ca.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_cz.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_da.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_el.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_es.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_eu.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_de.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_fa.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_fi.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_fr.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_ga.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_gl.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_hi.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_hu.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_hy.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_id.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_it.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_ja.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_lv.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_nl.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_no.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_pt.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_ro.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_ru.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_sv.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_th.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stopwords_tr.txt");
    
    putConfig(zkClient, SOLR_CONF_DIR, "lang/contractions_ca.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/contractions_fr.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/contractions_ga.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "lang/contractions_it.txt");
    
    putConfig(zkClient, SOLR_CONF_DIR, "lang/stemdict_nl.txt");
    
    putConfig(zkClient, SOLR_CONF_DIR, "lang/hyphenations_ga.txt");
    
    putConfig(zkClient, SOLR_CONF_DIR, "stopwords.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "protwords.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "currency.xml");
    putConfig(zkClient, SOLR_CONF_DIR, "open-exchange-rates.json");
    putConfig(zkClient, SOLR_CONF_DIR, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "old_synonyms.txt");
    putConfig(zkClient, SOLR_CONF_DIR, "synonyms.txt");
    zkClient.close();
  }
  
}
