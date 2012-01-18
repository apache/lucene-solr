/**
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
package org.apache.solr.handler;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.junit.BeforeClass;
import org.junit.Test;

public class BinaryUpdateRequestHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testRequestParams() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam(UpdateParams.COMMIT_WITHIN, "100");
    ureq.setParam(UpdateParams.OVERWRITE, Boolean.toString(false));

    BinaryRequestWriter brw = new BinaryRequestWriter();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    SolrQueryResponse rsp = new SolrQueryResponse();
    BinaryUpdateRequestHandler handler = new BinaryUpdateRequestHandler();
    SolrQueryRequest req = req();
    ContentStreamLoader csl = handler.newLoader(req, p);

    csl.load(req, rsp, brw.getContentStream(ureq));

    AddUpdateCommand add = p.addCommands.get(0);
    System.out.println(add.solrDoc);
    assertEquals(false, add.overwrite);
    assertEquals(100, add.commitWithin);

    req.close();
  }
}
