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

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides functionality required to invoke operations for Solr collection replicas
 * and process the results of those operations.
 * TODO - Needs to cleanup this class as well as OverseerCollectionMessageHandler
 */
public class ShardRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ShardHandler shardHandler;
  private final String adminPath;
  private final ZkStateReader zkStateReader;
  private final String asyncId;
  private final Map<String, String> requestMap = new HashMap<>();

  public ShardRequestProcessor(ShardHandler shardHandler, String adminPath, ZkStateReader zkStateReader, String asyncId) {
    this.shardHandler = shardHandler;
    this.adminPath = adminPath;
    this.zkStateReader = zkStateReader;
    this.asyncId = asyncId;
  }

  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  public void sendShardRequest(String nodeName, ModifiableSolrParams params) {
    if (asyncId != null) {
      String coreAdminAsyncId = asyncId + Math.abs(System.nanoTime());
      params.set(ASYNC, coreAdminAsyncId);
      requestMap.put(nodeName, coreAdminAsyncId);
    }

    ShardRequest sreq = new ShardRequest();
    params.set("qt", adminPath);
    sreq.purpose = 1;
    String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.nodeName = nodeName;
    sreq.params = params;

    shardHandler.submit(sreq, replica, sreq.params);
  }

  public NamedList processResponses(boolean abortOnError, String msgOnError, Set<String> okayExceptions) {
    NamedList results = new NamedList<>();
    // Processes all shard responses
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp, okayExceptions);
        Throwable exception = srsp.getException();
        if (abortOnError && exception != null) {
          // drain pending requests
          while (srsp != null) {
            srsp = shardHandler.takeCompletedOrError();
          }
          throw new SolrException(ErrorCode.SERVER_ERROR, msgOnError, exception);
        }
      }
    } while (srsp != null);

    // If request is async wait for the core admin to complete before returning
    if (asyncId != null) {
      waitForAsyncCallsToComplete(requestMap, results);
      requestMap.clear();
    }

    return results;
  }

  private void processResponse(NamedList results, ShardResponse srsp, Set<String> okayExceptions) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard, okayExceptions);
  }

  @SuppressWarnings("unchecked")
  private void processResponse(NamedList results, Throwable e, String nodeName, SolrResponse solrResponse, String shard, Set<String> okayExceptions) {
    String rootThrowable = null;
    if (e instanceof RemoteSolrException) {
      rootThrowable = ((RemoteSolrException) e).getRootThrowable();
    }

    if (e != null && (rootThrowable == null || !okayExceptions.contains(rootThrowable))) {
      log.error("Error from shard: " + shard, e);

      SimpleOrderedMap failure = (SimpleOrderedMap) results.get("failure");
      if (failure == null) {
        failure = new SimpleOrderedMap();
        results.add("failure", failure);
      }

      failure.add(nodeName, e.getClass().getName() + ":" + e.getMessage());

    } else {

      SimpleOrderedMap success = (SimpleOrderedMap) results.get("success");
      if (success == null) {
        success = new SimpleOrderedMap();
        results.add("success", success);
      }

      success.add(nodeName, solrResponse.getResponse());
    }
  }

  @SuppressWarnings("unchecked")
  private void waitForAsyncCallsToComplete(Map<String, String> requestMap, NamedList results) {
    for (String k:requestMap.keySet()) {
      log.debug("I am Waiting for :{}/{}", k, requestMap.get(k));
      results.add(requestMap.get(k), waitForCoreAdminAsyncCallToComplete(k, requestMap.get(k)));
    }
  }

  private NamedList waitForCoreAdminAsyncCallToComplete(String nodeName, String requestId) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;
    do {
      sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          NamedList results = new NamedList();
          processResponse(results, srsp, Collections.emptySet());
          String r = (String) srsp.getSolrResponse().getResponse().get("STATUS");
          if (r.equals("running")) {
            log.debug("The task is still RUNNING, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("completed")) {
            log.debug("The task is COMPLETED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("failed")) {
            // TODO: Improve this. Get more information.
            log.debug("The task is FAILED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("notfound")) {
            log.debug("The task is notfound, retry");
            if (counter++ < 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
              break;
            }
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request for requestId: " + requestId + "" + srsp.getSolrResponse().getResponse().get("STATUS") +
                "retried " + counter + "times");
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request " + srsp.getSolrResponse().getResponse().get("STATUS"));
          }
        }
      } while (srsp != null);
    } while(true);
  }
}
