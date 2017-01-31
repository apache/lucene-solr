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

package org.apache.solr.handler.admin;


import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;

import static java.util.Collections.EMPTY_MAP;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.handler.admin.TestCollectionAPIs.compareOutput;

public class TestConfigsApi extends SolrTestCaseJ4 {


  public void testCommands() throws Exception {

    ConfigSetsHandler handler = new ConfigSetsHandler(null) {
      @Override
      protected void sendToZk(SolrQueryResponse rsp,
                              ConfigSetOperation operation,
                              Map<String, Object> result)
          throws KeeperException, InterruptedException {
        result.put(QUEUE_OPERATION, operation.action.toLower());
        rsp.add(ZkNodeProps.class.getName(), new ZkNodeProps(result));
      }
    };
    ApiBag apiBag = new ApiBag(false);
    for (Api api : handler.getApis()) apiBag.register(api, EMPTY_MAP);
    compareOutput(apiBag, "/cluster/configs/sample", DELETE, null, null,
        "{name :sample, operation:delete}");

    compareOutput(apiBag, "/cluster/configs", POST, "{create:{name : newconf, baseConfigSet: sample }}", null,
        "{operation:create, name :newconf,  baseConfigSet: sample }");
  }
}
