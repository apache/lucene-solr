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
package org.apache.solr.handler.loader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.junit.BeforeClass;

public class JavabinLoaderTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  /**
   * Verifies the isLastDocInBatch flag gets set correctly for a batch of docs and for a request with a single doc.
   */
  public void testLastDocInBatchFlag() throws Exception {
    doTestLastDocInBatchFlag(1); // single doc
    doTestLastDocInBatchFlag(2); // multiple docs
  }

  protected void doTestLastDocInBatchFlag(int numDocsInBatch) throws Exception {
    List<SolrInputDocument> batch = new ArrayList<>(numDocsInBatch);
    for (int d=0; d < numDocsInBatch; d++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", String.valueOf(d));
      batch.add(doc);
    }

    UpdateRequest updateRequest = new UpdateRequest();
    if (batch.size() > 1) {
      updateRequest.add(batch);
    } else {
      updateRequest.add(batch.get(0));
    }

    // client-side SolrJ would do this ...
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    (new JavaBinUpdateRequestCodec()).marshal(updateRequest, os);

    // need to override the processAdd method b/c JavabinLoader calls
    // clear on the addCmd after it is passed on to the handler ... a simple clone will suffice for this test
    BufferingRequestProcessor mockUpdateProcessor = new BufferingRequestProcessor(null) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        addCommands.add((AddUpdateCommand)cmd.clone());
      }
    };

    SolrQueryRequest req = req();
    (new JavabinLoader()).load(req,
        new SolrQueryResponse(),
        new ContentStreamBase.ByteArrayStream(os.toByteArray(), "test"),
        mockUpdateProcessor);
    req.close();

    assertTrue(mockUpdateProcessor.addCommands.size() == numDocsInBatch);
    for (int i=0; i < numDocsInBatch-1; i++)
      assertFalse(mockUpdateProcessor.addCommands.get(i).isLastDocInBatch); // not last doc in batch

    // last doc should have the flag set
    assertTrue(mockUpdateProcessor.addCommands.get(batch.size()-1).isLastDocInBatch);
  }
}
