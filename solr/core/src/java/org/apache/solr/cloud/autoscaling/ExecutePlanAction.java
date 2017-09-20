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

package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for executing cluster operations read from the {@link ActionContext}'s properties
 * with the key name "operations"
 */
public class ExecutePlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PREFIX = "op-";

  static final int DEFAULT_TASK_TIMEOUT_SECONDS = 120;

  @Override
  public void process(TriggerEvent event, ActionContext context) {
    log.debug("-- processing event: {} with context properties: {}", event, context.getProperties());
    CoreContainer container = context.getCoreContainer();
    SolrZkClient zkClient = container.getZkController().getZkClient();
    List<SolrRequest> operations = (List<SolrRequest>) context.getProperty("operations");
    if (operations == null || operations.isEmpty()) {
      log.info("No operations to execute for event: {}", event);
      return;
    }
    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
        .withZkHost(container.getZkController().getZkServerAddress())
        .withHttpClient(container.getUpdateShardHandler().getHttpClient())
        .build()) {
      int counter = 0;
      for (SolrRequest operation : operations) {
        log.info("Executing operation: {}", operation.getParams());
        try {
          SolrResponse response = null;
          if (operation instanceof CollectionAdminRequest.AsyncCollectionAdminRequest) {
            CollectionAdminRequest.AsyncCollectionAdminRequest req = (CollectionAdminRequest.AsyncCollectionAdminRequest) operation;
            String asyncId = event.getSource() + '/' + event.getId() + '/' + counter;
            String znode = saveAsyncId(event, context, asyncId);
            log.debug("Saved requestId: {} in znode: {}", asyncId, znode);
            asyncId = req.processAsync(asyncId, cloudSolrClient);
            CollectionAdminRequest.RequestStatusResponse statusResponse = waitForTaskToFinish(cloudSolrClient, asyncId,
                DEFAULT_TASK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (statusResponse != null) {
              RequestStatusState state = statusResponse.getRequestStatus();
              if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED || state == RequestStatusState.NOT_FOUND) {
                try {
                  zkClient.delete(znode, -1, true);
                } catch (KeeperException e) {
                  log.warn("Unexpected exception while trying to delete znode: " + znode, e);
                }
              }
              response = statusResponse;
            }
          } else {
            response = operation.process(cloudSolrClient);
          }
          NamedList<Object> result = response.getResponse();
          context.getProperties().compute("responses", (s, o) -> {
            List<NamedList<Object>> responses = (List<NamedList<Object>>) o;
            if (responses == null)  responses = new ArrayList<>(operations.size());
            responses.add(result);
            return responses;
          });
        } catch (SolrServerException | HttpSolrClient.RemoteSolrException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unexpected exception executing operation: " + operation.getParams(), e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ExecutePlanAction was interrupted", e);
        } catch (KeeperException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to talk to ZooKeeper", e);
        }

        counter++;
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected IOException while processing event: " + event, e);
    }
  }

  static CollectionAdminRequest.RequestStatusResponse waitForTaskToFinish(CloudSolrClient cloudSolrClient, String requestId, long duration, TimeUnit timeUnit) throws SolrServerException, IOException, InterruptedException {
    long timeoutSeconds = timeUnit.toSeconds(duration);
    RequestStatusState state = RequestStatusState.NOT_FOUND;
    CollectionAdminRequest.RequestStatusResponse statusResponse = null;
    for (int i = 0; i < timeoutSeconds; i++) {
      try {
        statusResponse = CollectionAdminRequest.requestStatus(requestId).process(cloudSolrClient);
        state = statusResponse.getRequestStatus();
        if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
          log.info("Task with requestId={} finished with state={} in {}s", requestId, state, i * 5);
          CollectionAdminRequest.deleteAsyncId(requestId).process(cloudSolrClient);
          return statusResponse;
        } else if (state == RequestStatusState.NOT_FOUND) {
          // the request for this id was never actually submitted! no harm done, just bail out
          log.warn("Task with requestId={} was not found on overseer", requestId);
          CollectionAdminRequest.deleteAsyncId(requestId).process(cloudSolrClient);
          return statusResponse;
        }
      } catch (Exception e) {
        Throwable rootCause = ExceptionUtils.getRootCause(e);
        if (rootCause instanceof IllegalStateException && rootCause.getMessage().contains("Connection pool shut down"))  {
          throw e;
        }
        if (rootCause instanceof TimeoutException && rootCause.getMessage().contains("Could not connect to ZooKeeper")) {
          throw e;
        }
        log.error("Unexpected Exception while querying status of requestId=" + requestId, e);
      }
      if (i > 0 && i % 5 == 0) {
        log.debug("Task with requestId={} still not complete after {}s. Last state={}", requestId, i * 5, state);
      }
      TimeUnit.SECONDS.sleep(5);
    }
    log.debug("Task with requestId={} did not complete within 5 minutes. Last state={}", requestId, state);
    return statusResponse;
  }

  /**
   * Saves the given asyncId in ZK as a persistent sequential node.
   *
   * @return the path of the newly created node in ZooKeeper
   */
  private String saveAsyncId(TriggerEvent event, ActionContext context, String asyncId) throws InterruptedException, KeeperException {
    String parentPath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + context.getSource().getName() + "/" + getName();
    CoreContainer container = context.getCoreContainer();
    SolrZkClient zkClient = container.getZkController().getZkClient();
    try {
      zkClient.makePath(parentPath, new byte[0], CreateMode.PERSISTENT, true);
    } catch (KeeperException.NodeExistsException e) {
      // ignore
    }
    return zkClient.create(parentPath + "/" + PREFIX, Utils.toJSON(Collections.singletonMap("requestid", asyncId)), CreateMode.PERSISTENT_SEQUENTIAL, true);
  }

}
