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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// See SOLR-6640
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.Nightly
public class RecoveryAfterSoftCommitTest extends SolrCloudBridgeTestCase {
  private static final int MAX_BUFFERED_DOCS = 2, ULOG_NUM_RECORDS_TO_KEEP = 2;

  public RecoveryAfterSoftCommitTest() {

  }

  @BeforeClass
  public static void beforeTests() {
    sliceCount = 1;
    numJettys = 2;
    replicationFactor = 2;
    enableProxy = true;
    uploadSelectCollection1Config = true;
    solrconfigString = "solrconfig.xml";
    schemaString = "schema.xml";
    System.setProperty("solr.tests.maxBufferedDocs", String.valueOf(MAX_BUFFERED_DOCS));
    System.setProperty("solr.ulog.numRecordsToKeep", String.valueOf(ULOG_NUM_RECORDS_TO_KEEP));
    // avoid creating too many files, see SOLR-7421
    System.setProperty("useCompoundFile", "true");
  }

  @AfterClass
  public static void afterTest()  {

  }

  @Test
  public void test() throws Exception {
    // flush twice
    int i = 0;
    for (; i<MAX_BUFFERED_DOCS + 1; i++) {
      SolrInputDocument document = new SolrInputDocument();
      document.addField("id", String.valueOf(i));
      document.addField("a_t", "text_" + i);
      cloudClient.add(document);
    }

    // soft-commit so searchers are open on un-committed but flushed segment files
    AbstractUpdateRequest request = new UpdateRequest().setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true, true);
    cloudClient.request(request);

    //Replica notLeader = ensureAllReplicasAreActive(DEFAULT_COLLECTION, "shard1", 1, 2, 30).get(0);
    // ok, now introduce a network partition between the leader and the replica
    Replica notLeader = cluster.getNonLeaderReplica(COLLECTION);
    SocketProxy proxy = cluster.getProxyForReplica(notLeader);

    proxy.close();

    // add more than ULOG_NUM_RECORDS_TO_KEEP docs so that peer sync cannot be used for recovery
    int MAX_DOCS = 2 + MAX_BUFFERED_DOCS + ULOG_NUM_RECORDS_TO_KEEP;
    for (; i < MAX_DOCS; i++) {
      SolrInputDocument document = new SolrInputDocument();
      document.addField("id", String.valueOf(i));
      document.addField("a_t", "text_" + i);
      cloudClient.add(document);
    }

    // Have the partition last at least 1 sec
    // While this gives the impression that recovery is timing related, this is
    // really only
    // to give time for the state to be written to ZK before the test completes.
    // In other words,
    // without a brief pause, the test finishes so quickly that it doesn't give
    // time for the recovery process to kick-in
    Thread.sleep(2000L);

    proxy.reopen();

    cluster.waitForActiveCollection(COLLECTION, 1, 2);
  }
}

