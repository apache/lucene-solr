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
package org.apache.solr.cloud.autoscaling.sim;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

/**
 *
 */
@TimeoutSuite(millis = 48 * 3600 * 1000)
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.autoscaling.NodeLostTrigger=INFO;org.apache.client.solrj.cloud.autoscaling=DEBUG;org.apache.solr.cloud.autoscaling.ComputePlanAction=INFO;org.apache.solr.cloud.autoscaling.ExecutePlanAction=DEBUG;org.apache.solr.cloud.autoscaling.ScheduledTriggers=DEBUG")
//@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.autoscaling.NodeLostTrigger=INFO;org.apache.client.solrj.cloud.autoscaling=DEBUG;org.apache.solr.cloud.CloudTestUtils=TRACE")
public class TestSimExtremeIndexing extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int SPEED = 100;
  // use higher speed for larger scale tests
  // private static final int SPEED = 500;
  private static final int NUM_NODES = 200;

  private static final long BATCH_SIZE = 200000;

  private static final long NUM_BATCHES = 5000;
  //  ... or use this for a 1 trillion docs test
  //  private static final long NUM_BATCHES = 5000000;

  // tweak this threshold to test the number of splits
  private static final long ABOVE_SIZE = 20000000;

  // tweak this to allow more operations in one event
  private static final int MAX_OPS = 100;


  private static TimeSource timeSource;
  private static SolrClient solrClient;

  @Before
  public void setupCluster() throws Exception {
    configureCluster(NUM_NODES, TimeSource.get("simTime:" + SPEED));
    timeSource = cluster.getTimeSource();
    solrClient = cluster.simGetSolrClient();
    cluster.simSetUseSystemCollection(false);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    solrClient = null;
  }
  
  @After
  public void afterTest() throws Exception {
    shutdownCluster();
  }

  @Test
  public void testScaleUp() throws Exception {
    String collectionName = "testScaleUp_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(10);
    create.process(solrClient);
    
    CloudTestUtils.waitForState(cluster, collectionName, 90, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(2, 2, false, true));

    //long waitForSeconds = 3 + random().nextInt(5);
    long waitForSeconds = 1;
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'scaleUpTrigger'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'aboveDocs' : " + ABOVE_SIZE + "," +
        "'maxOps' : " + MAX_OPS + "," +
        "'enabled' : true," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name' : 'execute_plan', 'class' : '" + ExecutePlanAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    long batchSize = BATCH_SIZE;
    for (long i = 0; i < NUM_BATCHES; i++) {
      addDocs(collectionName, i * batchSize, batchSize);
      log.info(String.format(Locale.ROOT, "#### Total docs so far: %,d", ((i + 1) * batchSize)));
      timeSource.sleep(waitForSeconds);
    }
    timeSource.sleep(60000);
    QueryResponse rsp = solrClient.query(collectionName, params(CommonParams.Q, "*:*"));
    SolrDocumentList docs = rsp.getResults();
    assertNotNull(docs);
    assertEquals(docs.toString(), batchSize * NUM_BATCHES, docs.getNumFound());
  }

  private void addDocs(String collection, long start, long count) throws Exception {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParam("collection", collection);
    ureq.setDocIterator(new FakeDocIterator(start, count));
    solrClient.request(ureq);
  }

  // lightweight generator of fake documents
  // NOTE: this iterator only ever returns the same document, which works ok
  // for our "index update" simulation. Obviously don't use this for real indexing.
  private static class FakeDocIterator implements Iterator<SolrInputDocument> {
    final SolrInputDocument doc = new SolrInputDocument();
    final SolrInputField idField = new SolrInputField("id");

    final long start, count;

    long current, max;

    FakeDocIterator(long start, long count) {
      this.start = start;
      this.count = count;
      current = start;
      max = start + count;
      doc.put("id", idField);
      idField.setValue("foo");
    }

    @Override
    public boolean hasNext() {
      return current < max;
    }

    @Override
    public SolrInputDocument next() {
      current++;
      return doc;
    }
  }

}
