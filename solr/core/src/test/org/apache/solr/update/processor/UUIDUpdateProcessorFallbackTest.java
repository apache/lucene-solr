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

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.BeforeClass;

public class UUIDUpdateProcessorFallbackTest extends SolrTestCaseJ4 {

  Date now = new Date();

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema.xml");
  }

  public void testFallbackToUnique() throws Exception {

    // get all defaults
    SolrInputDocument d = processAdd("default-values-fallback-to-unique",
        doc(f("name", "Existing", "Values")));

    assertNotNull(d);

    assertNotNull(d.getFieldValue("id"));
    assertNotNull(UUID.fromString(d.getFieldValue("id").toString()));

    // get all defaults
    d = processAdd("default-values-fallback-to-unique-automatically",
        doc(f("name", "Existing", "Values")));

    assertNotNull(d);

    assertNotNull(d.getFieldValue("id"));
    assertNotNull(UUID.fromString(d.getFieldValue("id").toString()));

    // defaults already specified
    d = processAdd("default-values-fallback-to-unique",
        doc(f("timestamp", now),
            f("id", "550e8400-e29b-41d4-a716-446655440000"),
            f("processor_default_s", "I HAVE A VALUE"),
            f("processor_default_i", 12345),
            f("name", "Existing", "Values")));

    assertNotNull(d);

    assertEquals("550e8400-e29b-41d4-a716-446655440000",
        d.getFieldValue("id"));

    // defaults already specified //both config and request param not passed.
    d = processAdd("default-values-fallback-to-unique-automatically",
        doc(f("timestamp", now),
            f("id", "550e8400-e29b-41d4-a716-446655440000"),
            f("processor_default_s", "I HAVE A VALUE"),
            f("processor_default_i", 121),
            f("name", "Existing", "Values")));

    assertNotNull(d);

    assertEquals("550e8400-e29b-41d4-a716-446655440000",
        d.getFieldValue("id"));
    assertEquals(121, d.getFieldValue("processor_default_i"));
  }

  public void testRequesTParams() throws Exception {
    SolrInputDocument d = processAdd(null,
        doc(f("name", "Existing", "Values"), f( "id","75765")), params("processor", "uuid", "uuid.fieldName", "id_s"));

    assertNotNull(d);

    assertNotNull(d.getFieldValue("id_s"));
    assertNotNull(UUID.fromString(d.getFieldValue("id_s").toString()));



    // defaults already specified
    d = processAdd(null,
        doc(f("timestamp", now),
            f("id", "454435"),
            f("id_s", "550e8400-e29b-41d4-a716-446655440000"),
            f("processor_default_s", "I HAVE A VALUE"),
            f("processor_default_i", 121),
            f("name", "Existing", "Values"))
        , params("processor", "uuid", "uuid.fieldName", "id_s"));

    assertNotNull(d);

    assertEquals("550e8400-e29b-41d4-a716-446655440000",
        d.getFieldValue("id_s"));
    assertEquals(121, d.getFieldValue("processor_default_i"));
  }

  public void testProcessorPrefixReqParam() throws Exception {
    List<UpdateRequestProcessorFactory> processors = UpdateRequestProcessorChain.getReqProcessors("uuid", h.getCore());
    UpdateRequestProcessorFactory processorFactory = processors.get(0);
    assertTrue(processorFactory instanceof UUIDUpdateProcessorFactory);

    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams());
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.addField("random_s", "random_val");

    processorFactory.getInstance(req, rsp, null).processAdd(cmd);
    assertNotNull(cmd.solrDoc);
    assertNotNull(cmd.solrDoc.get("id"));
    assertNotNull(cmd.solrDoc.get("id").getValue());
  }

  /**
   * Convenience method for building up SolrInputDocuments
   */
  SolrInputDocument doc(SolrInputField... fields) {
    SolrInputDocument d = new SolrInputDocument();
    for (SolrInputField f : fields) {
      d.put(f.getName(), f);
    }
    return d;
  }

  /**
   * Convenience method for building up SolrInputFields
   */
  SolrInputField field(String name, float boost, Object... values) {
    SolrInputField f = new SolrInputField(name);
    for (Object v : values) {
      f.addValue(v);
    }
    return f;
  }

  /**
   * Convenience method for building up SolrInputFields with default boost
   */
  SolrInputField f(String name, Object... values) {
    return field(name, 1.0F, values);
  }


  /**
   * Runs a document through the specified chain, and returns the final
   * document used when the chain is completed (NOTE: some chains may
   * modify the document in place
   */

  SolrInputDocument processAdd(final String chain,
                               final SolrInputDocument docIn) throws IOException {
    return processAdd(chain, docIn, params());
  }

  SolrInputDocument processAdd(final String chain,
                               final SolrInputDocument docIn, SolrParams params)
      throws IOException {

    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = chain == null ?
        core.getUpdateProcessorChain(params) :
        core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();

    SolrQueryRequest req = new LocalSolrQueryRequest
        (core, params);
    try {
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req,rsp));
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = docIn;

      UpdateRequestProcessor processor = pc.createProcessor(req, rsp);
      processor.processAdd(cmd);

      return cmd.solrDoc;
    } finally {
      SolrRequestInfo.clearRequestInfo();
      req.close();
    }
  }
}
