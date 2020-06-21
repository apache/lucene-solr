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
package org.apache.solr.cloud.cdcr;

import java.util.Arrays;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrParams;
import org.junit.Test;

@Nightly
public class CdcrRequestHandlerTest extends BaseCdcrDistributedZkTest {

  @Override
  public void distribSetUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    createTargetCollection = false;     // we do not need the target cluster
    super.distribSetUp();
  }

  // check that the life-cycle state is properly synchronised across nodes
  @Test
  @ShardsFixed(num = 2)
  public void testLifeCycleActions() throws Exception {
    // check initial status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STOPPED, CdcrParams.BufferState.ENABLED);

    // send start action to first shard
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    @SuppressWarnings({"rawtypes"})
    NamedList status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
    assertEquals(CdcrParams.ProcessState.STARTED.toLower(), status.get(CdcrParams.ProcessState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STARTED, CdcrParams.BufferState.ENABLED);

    // Restart the leader of shard 1
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));

    // check status - the node that died should have picked up the original state
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STARTED, CdcrParams.BufferState.ENABLED);

    // send stop action to second shard
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.STOP);
    status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
    assertEquals(CdcrParams.ProcessState.STOPPED.toLower(), status.get(CdcrParams.ProcessState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STOPPED, CdcrParams.BufferState.ENABLED);
  }

  // check the checkpoint API
  @Test
  @ShardsFixed(num = 2)
  public void testCheckpointActions() throws Exception {
    // initial request on an empty index, must return -1
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    assertEquals(-1l, rsp.get(CdcrParams.CHECKPOINT));

    index(SOURCE_COLLECTION, getDoc(id, "a","test_i_dvo",10)); // shard 2

    // only one document indexed in shard 2, the checkpoint must be still -1
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    assertEquals(-1l, rsp.get(CdcrParams.CHECKPOINT));

    index(SOURCE_COLLECTION, getDoc(id, "b")); // shard 1

    // a second document indexed in shard 1, the checkpoint must come from shard 2
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    long checkpoint1 = (Long) rsp.get(CdcrParams.CHECKPOINT);
    long expected = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.SHARDCHECKPOINT).get(CdcrParams.CHECKPOINT);
    assertEquals(expected, checkpoint1);

    index(SOURCE_COLLECTION, getDoc(id, "c")); // shard 1

    // a third document indexed in shard 1, the checkpoint must still come from shard 2
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    assertEquals(checkpoint1, rsp.get(CdcrParams.CHECKPOINT));

    index(SOURCE_COLLECTION, getDoc(id, "d")); // shard 2

    // a fourth document indexed in shard 2, the checkpoint must come from shard 1
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    long checkpoint2 = (Long) rsp.get(CdcrParams.CHECKPOINT);
    expected = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.SHARDCHECKPOINT).get(CdcrParams.CHECKPOINT);
    assertEquals(expected, checkpoint2);

    // send a delete by id
    long pre_op = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.SHARDCHECKPOINT).get(CdcrParams.CHECKPOINT);
    deleteById(SOURCE_COLLECTION, Arrays.asList(new String[]{"c"})); //shard1
    // document deleted in shard1, checkpoint should come from shard2
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    long checkpoint3 = (Long) rsp.get(CdcrParams.CHECKPOINT);
    expected = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.SHARDCHECKPOINT).get(CdcrParams.CHECKPOINT);
    assertEquals(pre_op, expected);
    assertEquals(expected, checkpoint3);

    // send a in-place update
    SolrInputDocument in_place_doc = new SolrInputDocument();
    in_place_doc.setField(id, "a");
    in_place_doc.setField("test_i_dvo", ImmutableMap.of("inc", 10)); //shard2
    index(SOURCE_COLLECTION, in_place_doc);
    // document updated in shard2, checkpoint should come from shard1
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    long checkpoint4 = (Long) rsp.get(CdcrParams.CHECKPOINT);
    expected = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.SHARDCHECKPOINT).get(CdcrParams.CHECKPOINT);
    assertEquals(expected, checkpoint4);

    // send a delete by query
    deleteByQuery(SOURCE_COLLECTION, "*:*");

    // all the checkpoints must come from the DBQ
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.COLLECTIONCHECKPOINT);
    long checkpoint5= (Long) rsp.get(CdcrParams.CHECKPOINT);
    assertTrue(checkpoint5 > 0); // ensure that checkpoints from deletes are in absolute form
    checkpoint5 = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.SHARDCHECKPOINT).get(CdcrParams.CHECKPOINT);
    assertTrue(checkpoint5 > 0); // ensure that checkpoints from deletes are in absolute form
    checkpoint5 = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.SHARDCHECKPOINT).get(CdcrParams.CHECKPOINT);
    assertTrue(checkpoint5 > 0); // ensure that checkpoints from deletes are in absolute form


    // replication never started, lastProcessedVersion should be -1 for both shards
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.LASTPROCESSEDVERSION);
    long lastVersion = (Long) rsp.get(CdcrParams.LAST_PROCESSED_VERSION);
    assertEquals(-1l, lastVersion);

    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.LASTPROCESSEDVERSION);
    lastVersion = (Long) rsp.get(CdcrParams.LAST_PROCESSED_VERSION);
    assertEquals(-1l, lastVersion);
  }

  // check that the buffer state is properly synchronised across nodes
  @Test
  @ShardsFixed(num = 2)
  public void testBufferActions() throws Exception {
    // check initial status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STOPPED, CdcrParams.BufferState.ENABLED);

    // send disable buffer action to first shard
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.DISABLEBUFFER);
    @SuppressWarnings({"rawtypes"})
    NamedList status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
    assertEquals(CdcrParams.BufferState.DISABLED.toLower(), status.get(CdcrParams.BufferState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STOPPED, CdcrParams.BufferState.DISABLED);

    // Restart the leader of shard 1
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STOPPED, CdcrParams.BufferState.DISABLED);

    // send enable buffer action to second shard
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.ENABLEBUFFER);
    status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
    assertEquals(CdcrParams.BufferState.ENABLED.toLower(), status.get(CdcrParams.BufferState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STOPPED, CdcrParams.BufferState.ENABLED);
  }

}

