package org.apache.solr.cloud.rule;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.http.client.methods.HttpGet;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.update.UpdateShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.INVOKE;

/**
 * This is the context provided to the snitches to interact with the system. This is a per-node-per-snitch
 * instance.
 */
public class SnitchContext implements RemoteCallback {
  static final Logger log = LoggerFactory.getLogger(SnitchContext.class);
  private final Map<String, Object> tags = new HashMap<>();
  private String node;
  final SnitchInfo snitchInfo;
  Exception exception;


  SnitchContext(SnitchInfo perSnitch, String node) {
    this.snitchInfo = perSnitch;
    this.node = node;
  }

  public SnitchInfo getSnitchInfo() {
    return snitchInfo;
  }

  public Map<String, Object> getTags() {
    return tags;
  }

  public String getNode() {
    return node;
  }

  /**
   * make a call to solrnode/admin/cores with the given params and give a callback. This is designed to be
   * asynchronous because the system would want to batch the calls made to any given node
   *
   * @param node     The node for which this call is made
   * @param params   The params to be passed to the Snitch counterpart
   * @param klas     The  name of the class to be invoked in the remote node
   * @param callback The callback to be called when the response is obtained from remote node.
   *                 If this is passed as null the entire response map will be added as tags
   */
  public void invokeRemote(String node, ModifiableSolrParams params, String klas, RemoteCallback callback) {
    if (callback == null) callback = this;
    String url = snitchInfo.getCoreContainer().getZkController().getZkStateReader().getBaseUrlForNodeName(node);
    params.add("class", klas);
    params.add(ACTION, INVOKE.toString());
    //todo batch all requests to the same server

    try {
      SimpleSolrResponse rsp = invoke(snitchInfo.getCoreContainer().getUpdateShardHandler(), url, CoreContainer.CORES_HANDLER_PATH, params);
      Map<String, Object> returnedVal = (Map<String, Object>) rsp.getResponse().get(klas);
      if(exception == null){
//        log this
      } else {
        callback.remoteCallback(SnitchContext.this,returnedVal);
      }
      callback.remoteCallback(this, returnedVal);
    } catch (Exception e) {
      log.error("Unable to invoke snitch counterpart", e);
      exception = e;
    }
  }


  public SimpleSolrResponse invoke(UpdateShardHandler shardHandler,  final String url, String path, SolrParams params)
      throws IOException, SolrServerException {
    GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.GET, path, params);
    try (HttpSolrClient client = new HttpSolrClient(url, shardHandler.getHttpClient(), new BinaryResponseParser())) {
      NamedList<Object> rsp = client.request(request);
      request.response.nl = rsp;
      return request.response;
    }
  }


  @Override
  public void remoteCallback(SnitchContext ctx, Map<String, Object> returnedVal) {
    tags.putAll(returnedVal);
  }

  public String getErrMsg() {
    return exception == null ? null : exception.getMessage();
  }

  public static abstract class SnitchInfo {
    private final Map<String, Object> conf;

    SnitchInfo(Map<String, Object> conf) {
      this.conf = conf;
    }

    public abstract Set<String> getTagNames();

    public abstract CoreContainer getCoreContainer();
  }
}
