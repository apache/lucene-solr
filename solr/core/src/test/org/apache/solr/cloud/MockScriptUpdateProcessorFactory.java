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

import java.io.IOException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * <p>
 * The scripting update processor capability is something that is only allowed by a trusted
 * configSet.   The actual code lives in the /contrib/scripting project, however the test
 * for trusted configsets lives in TestConfigSetsAPI.   This class is meant to simulate the
 * ScriptUpdateProcessorFactory for this test.
 * </p>
*/
public class MockScriptUpdateProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware {

  @Override
  public void inform(SolrCore core) {
    if (!core.getCoreDescriptor().isConfigSetTrusted()) {
      throw new SolrException(ErrorCode.UNAUTHORIZED, "The configset for this collection was uploaded without any authentication in place,"
          + " and this operation is not available for collections with untrusted configsets. To use this component, re-upload the configset"
          + " after enabling authentication and authorization.");
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new MockScriptUpdateRequestProcessor(next);
  }

  private static class MockScriptUpdateRequestProcessor extends UpdateRequestProcessor {

    public MockScriptUpdateRequestProcessor(UpdateRequestProcessor next) {
      super(next);
    }
    /**
     * @param cmd the update command in input containing the Document to classify
     * @throws IOException If there is a low-level I/O error
     */
    @Override
    public void processAdd(AddUpdateCommand cmd)
        throws IOException {
      SolrInputDocument doc = cmd.getSolrInputDocument();

      doc.setField("script_added_i", "42");
      super.processAdd(cmd);
    }

  }

}
