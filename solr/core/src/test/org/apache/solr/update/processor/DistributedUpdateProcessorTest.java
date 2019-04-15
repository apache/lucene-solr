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

package org.apache.solr.update.processor;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateLog;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedUpdateProcessorTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solr/collection1/conf/solrconfig.xml","solr/collection1/conf/schema-minimal.xml");
  }

  @Test
  public void testShouldBufferUpdateZk() {
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams());
    DistributedUpdateProcessor processor = new DistributedUpdateProcessor(
        req, null, null, null);
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    // applying buffer updates, isReplayOrPeerSync flag doesn't matter
    assertFalse(processor.shouldBufferUpdate(cmd, false, UpdateLog.State.APPLYING_BUFFERED));
    assertFalse(processor.shouldBufferUpdate(cmd, true, UpdateLog.State.APPLYING_BUFFERED));

    assertTrue(processor.shouldBufferUpdate(cmd, false, UpdateLog.State.BUFFERING));
    // this is not an buffer updates and it depend on other updates
    cmd.prevVersion = 10;
    assertTrue(processor.shouldBufferUpdate(cmd, false, UpdateLog.State.APPLYING_BUFFERED));
  }

}
