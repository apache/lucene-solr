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

import java.util.Date;
import java.util.UUID;
import java.util.Arrays;
import java.io.IOException;

import org.apache.solr.SolrTestCaseJ4;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.solr.update.AddUpdateCommand;

import org.junit.BeforeClass;

public class DefaultValueUpdateProcessorTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }

  public void testDefaults() throws Exception {
    SolrInputDocument d = null;

    Date now = new Date();

    // get all defaults
    d = processAdd("default-values",
                   doc(f("id", "1111"),
                       f("name", "Existing", "Values")));
    
    assertNotNull(d);
    
    assertEquals("X", d.getFieldValue("processor_default_s"));
    assertEquals(42, d.getFieldValue("processor_default_i"));
    assertNotNull(d.getFieldValue("uuid"));
    assertNotNull(UUID.fromString(d.getFieldValue("uuid").toString()));
    assertNotNull(d.getFieldValue("timestamp"));
    assertTrue("timestamp not a date: " + 
               d.getFieldValue("timestamp").getClass(), 
               d.getFieldValue("timestamp") instanceof Date);
    assertEquals(Arrays.asList("Existing","Values"), 
                   d.getFieldValues("name"));
    
    // defaults already specified
    d = processAdd("default-values",
                   doc(f("id", "1111"),
                       f("timestamp", now),
                       f("uuid", "550e8400-e29b-41d4-a716-446655440000"),
                       f("processor_default_s", "I HAVE A VALUE"),
                       f("processor_default_i", 12345),
                       f("name", "Existing", "Values")));
    
    assertNotNull(d);
    
    assertEquals("I HAVE A VALUE", d.getFieldValue("processor_default_s"));
    assertEquals(12345, d.getFieldValue("processor_default_i"));
    assertEquals("550e8400-e29b-41d4-a716-446655440000",
                 d.getFieldValue("uuid"));
    assertEquals(now, d.getFieldValue("timestamp"));
    assertEquals(Arrays.asList("Existing","Values"), 
                 d.getFieldValues("name"));
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
  SolrInputField field(String name, Object... values) {
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
    return field(name, values);
  }


  /**
   * Runs a document through the specified chain, and returns the final 
   * document used when the chain is completed (NOTE: some chains may 
   * modify the document in place
   */
  SolrInputDocument processAdd(final String chain, 
                               final SolrInputDocument docIn) 
    throws IOException {

    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();

    SolrQueryRequest req = new LocalSolrQueryRequest
      (core, new ModifiableSolrParams());
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
