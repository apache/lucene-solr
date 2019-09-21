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
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.junit.BeforeClass;

import java.io.IOException;

public class IgnoreCommitOptimizeUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema.xml");
  }

  public void testIgnoreCommit() throws Exception {
    // verify that the processor returns an error if it receives a commit
    SolrQueryResponse rsp = processCommit("ignore-commit-from-client-403", false);
    assertNotNull("Sending a commit should have resulted in an exception in the response", rsp.getException());

    rsp = processCommit("ignore-commit-from-client-200", false);
    Exception shouldBeNull = rsp.getException();
    assertNull("Sending a commit should NOT have resulted in an exception in the response: "+shouldBeNull, shouldBeNull);

    rsp = processCommit("ignore-optimize-only-from-client-403", true);
    assertNotNull("Sending an optimize should have resulted in an exception in the response", rsp.getException());
    // commit should happen if DistributedUpdateProcessor.COMMIT_END_POINT == true
    rsp = processCommit("ignore-commit-from-client-403", false, Boolean.TRUE);
    shouldBeNull = rsp.getException();
    assertNull("Sending a commit should NOT have resulted in an exception in the response: "+shouldBeNull, shouldBeNull);
  }

  SolrQueryResponse processCommit(final String chain, boolean optimize) throws IOException {
    return processCommit(chain, optimize, null);
  }

  SolrQueryResponse processCommit(final String chain, boolean optimize, Boolean commitEndPoint) throws IOException {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());

    if (commitEndPoint != null) {
      ((ModifiableSolrParams)req.getParams()).set(
          DistributedUpdateProcessor.COMMIT_END_POINT, commitEndPoint.booleanValue());
    }

    try {
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req,rsp));
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
      cmd.optimize = optimize;
      UpdateRequestProcessor processor = pc.createProcessor(req, rsp);
      processor.processCommit(cmd);
    } finally {
      SolrRequestInfo.clearRequestInfo();
      req.close();
    }
    return rsp;
  }
}
