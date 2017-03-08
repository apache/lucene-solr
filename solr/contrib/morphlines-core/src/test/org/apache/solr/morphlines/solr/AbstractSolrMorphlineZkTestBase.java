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
import java.util.Locale;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.junit.Before;
import org.junit.BeforeClass;
import org.kitesdk.morphline.api.Collector;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.stdlib.PipeBuilder;

public abstract class AbstractSolrMorphlineZkTestBase extends SolrCloudTestCase {

  protected static final String COLLECTION = "collection1";

  protected static final int TIMEOUT = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", SOLR_CONF_DIR.toPath())
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .processAndWait(cluster.getSolrClient(), TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
  }

  protected static final String RESOURCES_DIR = getFile("morphlines-core.marker").getParent();
  private static final File SOLR_CONF_DIR = new File(RESOURCES_DIR + "/solr/collection1/conf");

  protected Collector collector;
  protected Command morphline;
  
  @BeforeClass
  public static void setupClass() throws Exception {

    assumeFalse("This test fails on UNIX with Turkish default locale (https://issues.apache.org/jira/browse/SOLR-6387)",
        new Locale("tr").getLanguage().equals(Locale.getDefault().getLanguage()));

  }

  @Before
  public void setup() throws Exception {
    collector = new Collector();
  }

  protected void commit() throws Exception {
    Notifications.notifyCommitTransaction(morphline);
  }
  
  protected Command parse(String file) throws IOException {
    return parse(file, COLLECTION);
  }
  
  protected Command parse(String file, String collection) throws IOException {
    SolrLocator locator = new SolrLocator(createMorphlineContext());
    locator.setCollectionName(collection);
    locator.setZkHost(cluster.getZkServer().getZkAddress());
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
  
}
