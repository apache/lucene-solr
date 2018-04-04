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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

import java.io.IOException;

public class UpdateProcessorTestBase extends SolrTestCaseJ4 {

  /**
   * Runs a document through the specified chain, and returns the final
   * document used when the chain is completed (NOTE: some chains may
   * modify the document in place
   */
  protected SolrInputDocument processAdd(final String chain,
                                         final SolrInputDocument docIn)
    throws IOException {

    return processAdd(chain, new ModifiableSolrParams(), docIn);
  }

  /**
   * Runs a document through the specified chain, and returns the final
   * document used when the chain is completed (NOTE: some chains may
   * modify the document in place
   */
  protected SolrInputDocument processAdd(final String chain,
                                         final SolrParams requestParams,
                                         final SolrInputDocument docIn)
    throws IOException {

    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();

    SolrQueryRequest req = new LocalSolrQueryRequest(core, requestParams);
    try {
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = docIn;

      UpdateRequestProcessor processor = pc.createProcessor(req, rsp);
      if (null != processor) {
        // test chain might be empty or short circuited.
        processor.processAdd(cmd);
      }

      return cmd.solrDoc;
    } finally {
      SolrRequestInfo.clearRequestInfo();
      req.close();
    }
  }

  protected void processCommit(final String chain) throws IOException {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();

    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());

    CommitUpdateCommand cmd = new CommitUpdateCommand(req,false);
    UpdateRequestProcessor processor = pc.createProcessor(req, rsp);
    try {
      processor.processCommit(cmd);
    } finally {
      req.close();
    }
  }

  protected void processDeleteById(final String chain, String id) throws IOException {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();

    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());

    DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
    cmd.setId(id);
    UpdateRequestProcessor processor = pc.createProcessor(req, rsp);
    try {
      processor.processDelete(cmd);
    } finally {
      req.close();
    }
  }

  protected void finish(final String chain) throws IOException {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());

    UpdateRequestProcessor processor = pc.createProcessor(req, rsp);
    try {
      processor.finish();
    } finally {
      IOUtils.closeQuietly(processor);
      req.close();
    }
  }


  /**
   * Convenience method for building up SolrInputDocuments
   */
  final SolrInputDocument doc(SolrInputField... fields) {
    SolrInputDocument d = new SolrInputDocument();
    for (SolrInputField f : fields) {
      d.put(f.getName(), f);
    }
    return d;
  }

  /**
   * Convenience method for building up SolrInputFields
   */
  final SolrInputField field(String name, Object... values) {
    SolrInputField f = new SolrInputField(name);
    for (Object v : values) {
      f.addValue(v);
    }
    return f;
  }

  /**
   * Convenience method for building up SolrInputFields with default boost
   */
  final SolrInputField f(String name, Object... values) {
    return field(name, values);
  }
}
