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

package org.apache.solr.cloud.rule;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.cloud.rule.RemoteCallback;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.INVOKE;

public class ServerSnitchContext extends SnitchContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CoreContainer coreContainer;
  public ServerSnitchContext(SnitchInfo perSnitch,
                             String node, Map<String, Object> session,
                             CoreContainer coreContainer) {
    super(perSnitch, node, session);
    this.coreContainer = coreContainer;
  }


  public Map getZkJson(String path) throws KeeperException, InterruptedException {
    if (coreContainer.isZooKeeperAware()) {
      return Utils.getJson(coreContainer.getZkController().getZkClient(), path, true);
    } else {
      return Collections.emptyMap();
    }
  }

  public void invokeRemote(String node, ModifiableSolrParams params, String klas, RemoteCallback callback) {
    if (callback == null) callback = this;
    params.add("class", klas);
    params.add(ACTION, INVOKE.toString());
    //todo batch all requests to the same server

    try {
      SimpleSolrResponse rsp = invoke(node, CommonParams.CORES_HANDLER_PATH, params);
      Map<String, Object> returnedVal = (Map<String, Object>) rsp.getResponse().get(klas);
      if(exception == null){
//        log this
      } else {
        callback.remoteCallback(ServerSnitchContext.this,returnedVal);
      }
      callback.remoteCallback(this, returnedVal);
    } catch (Exception e) {
      log.error("Unable to invoke snitch counterpart", e);
      exception = e;
    }
  }

  public SimpleSolrResponse invoke(String solrNode, String path, SolrParams params)
      throws IOException, SolrServerException {
    String url = coreContainer.getZkController().getZkStateReader().getBaseUrlForNodeName(solrNode);
    UpdateShardHandler shardHandler = coreContainer.getUpdateShardHandler();
    GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.GET, path, params);
    try (HttpSolrClient client = new HttpSolrClient.Builder(url).withHttpClient(shardHandler.getHttpClient())
        .withResponseParser(new BinaryResponseParser()).build()) {
      NamedList<Object> rsp = client.request(request);
      request.response.nl = rsp;
      return request.response;
    }
  }

}
