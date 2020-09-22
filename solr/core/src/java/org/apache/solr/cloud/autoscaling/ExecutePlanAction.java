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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.TestInjection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for executing cluster operations read from the {@link ActionContext}'s properties
 * with the key name "operations".
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class ExecutePlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PREFIX = "op-";

  static final int DEFAULT_TASK_TIMEOUT_SECONDS = 120;
  public static final String TASK_TIMEOUT_SECONDS = "taskTimeoutSeconds";
  public static final String TASK_TIMEOUT_FAIL = "taskTimeoutFail";

  int taskTimeoutSeconds;
  boolean taskTimeoutFail;

  public ExecutePlanAction() {
    TriggerUtils.validProperties(validProperties, TASK_TIMEOUT_SECONDS, TASK_TIMEOUT_FAIL);
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    String str = String.valueOf(properties.getOrDefault(TASK_TIMEOUT_SECONDS, DEFAULT_TASK_TIMEOUT_SECONDS));
    taskTimeoutSeconds = Integer.parseInt(str);
    str = String.valueOf(properties.getOrDefault(TASK_TIMEOUT_FAIL, false));
    taskTimeoutFail = Boolean.parseBoolean(str);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("-- processing event: {} with context properties: {}", event, context.getProperties());
    }
    SolrCloudManager cloudManager = context.getCloudManager();
    List<SolrRequest> operations = (List<SolrRequest>) context.getProperty("operations");
    if (operations == null || operations.isEmpty()) {
      log.info("No operations to execute for event: {}", event);
      return;
    }
    try {
      int counter = 0;
      for (SolrRequest operation : operations) {
        if (log.isDebugEnabled()) {
          log.debug("Executing operation: {}", operation.getParams());
        }
        try {
          SolrResponse response = null;
          if (operation instanceof CollectionAdminRequest.AsyncCollectionAdminRequest) {
            CollectionAdminRequest.AsyncCollectionAdminRequest req = (CollectionAdminRequest.AsyncCollectionAdminRequest) operation;
            // waitForFinalState so that the end effects of operations are visible
            req.setWaitForFinalState(true);
            String asyncId = event.getSource() + '/' + event.getId() + '/' + counter;
            String znode = saveAsyncId(cloudManager.getDistribStateManager(), event, asyncId);
            log.trace("Saved requestId: {} in znode: {}", asyncId, znode);
            // TODO: find a better way of using async calls using dataProvider API !!!
            req.setAsyncId(asyncId);
            if (TestInjection.delayInExecutePlanAction != null) {
              cloudManager.getTimeSource().sleep(TestInjection.delayInExecutePlanAction);
            }
            CollectionAdminRequest.RequestStatusResponse statusResponse = null;
            RequestStatusState state = RequestStatusState.FAILED;
            if (!TestInjection.failInExecutePlanAction) {
              SolrResponse asyncResponse = cloudManager.request(req);
              if (asyncResponse.getResponse().get("error") != null) {
                throw new IOException("" + asyncResponse.getResponse().get("error"));
              }
              asyncId = (String)asyncResponse.getResponse().get("requestid");
              statusResponse = waitForTaskToFinish(cloudManager, asyncId,
                  taskTimeoutSeconds, TimeUnit.SECONDS);
            }
            if (statusResponse != null) {
              state = statusResponse.getRequestStatus();
              // overwrite to test a long-running task
              if (TestInjection.delayInExecutePlanAction != null &&
                  TestInjection.delayInExecutePlanAction > TimeUnit.MILLISECONDS.convert(taskTimeoutSeconds, TimeUnit.SECONDS)) {
                state = RequestStatusState.RUNNING;
              }
              if (TestInjection.failInExecutePlanAction) {
                state = RequestStatusState.FAILED;
              }
              // should we accept partial success here? i.e. some operations won't be completed
              // successfully but the event processing will still be declared a success
              if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED || state == RequestStatusState.NOT_FOUND) {
                // remove pending task marker for this request
                try {
                  cloudManager.getDistribStateManager().removeData(znode, -1);
                } catch (Exception e) {
                  log.warn("Unexpected exception while trying to delete znode: {}", znode, e);
                }
              }
              response = statusResponse;
            }
            if (state == RequestStatusState.RUNNING || state == RequestStatusState.SUBMITTED) {
              String msg = String.format(Locale.ROOT, "Task %s is still running after " + taskTimeoutSeconds + " seconds. Consider increasing " +
                      TASK_TIMEOUT_SECONDS + " action property or `waitFor` of the trigger %s. Operation: %s",
                  asyncId, event.source, req);
              if (taskTimeoutFail) {
                throw new IOException(msg);
              } else {
                log.warn(msg);
              }
            } else if (state == RequestStatusState.FAILED) {
              // remove it as a pending task
              try {
                cloudManager.getDistribStateManager().removeData(znode, -1);
              } catch (Exception e) {
                log.warn("Unexpected exception while trying to delete znode: {}", znode, e);
              }
              throw new IOException("Task " + asyncId + " failed: " + (statusResponse != null ? statusResponse : " timed out. Operation: " + req));
            }
          } else {
            // generic response - can't easily determine success or failure
            response = cloudManager.request(operation);
          }
          NamedList<Object> result = response.getResponse();
          context.getProperties().compute("responses", (s, o) -> {
            @SuppressWarnings({"unchecked"})
            List<NamedList<Object>> responses = (List<NamedList<Object>>) o;
            if (responses == null)  responses = new ArrayList<>(operations.size());
            responses.add(result);
            return responses;
          });
          counter++;
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unexpected exception executing operation: " + operation.getParams(), e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ExecutePlanAction was interrupted", e);
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unexpected exception executing operation: " + operation.getParams(), e);
        }
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected exception while processing event: " + event, e);
    }
  }


  static CollectionAdminRequest.RequestStatusResponse waitForTaskToFinish(SolrCloudManager cloudManager, String requestId, long duration, TimeUnit timeUnit) throws IOException, InterruptedException {
    long timeoutSeconds = timeUnit.toSeconds(duration);
    RequestStatusState state = RequestStatusState.NOT_FOUND;
    CollectionAdminRequest.RequestStatusResponse statusResponse = null;
    for (int i = 0; i < timeoutSeconds; i++) {
      try {
        statusResponse = (CollectionAdminRequest.RequestStatusResponse)cloudManager.request(CollectionAdminRequest.requestStatus(requestId));
        state = statusResponse.getRequestStatus();
        if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
          log.trace("Task with requestId={} finished with state={} in {}s", requestId, state, i * 5);
          cloudManager.request(CollectionAdminRequest.deleteAsyncId(requestId));
          return statusResponse;
        } else if (state == RequestStatusState.NOT_FOUND) {
          // the request for this id was never actually submitted! no harm done, just bail out
          log.warn("Task with requestId={} was not found on overseer", requestId);
          cloudManager.request(CollectionAdminRequest.deleteAsyncId(requestId));
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
        if (rootCause instanceof SolrServerException) {
          throw e;
        }
        log.error("Unexpected Exception while querying status of requestId={}", requestId, e);
        throw e;
      }
      if (i > 0 && i % 5 == 0) {
        log.trace("Task with requestId={} still not complete after {}s. Last state={}", requestId, i * 5, state);
      }
      cloudManager.getTimeSource().sleep(5000);
    }
    log.debug("Task with requestId={} did not complete within {} seconds. Last state={}", timeoutSeconds, requestId, state);
    return statusResponse;
  }

  /**
   * Saves the given asyncId in ZK as a persistent sequential node. This allows us to wait for the completion
   * of pending tasks from this event in {@link ScheduledTriggers}
   * before starting the actions of the next event.
   *
   * @return the path of the newly created node in ZooKeeper
   */
  private String saveAsyncId(DistribStateManager stateManager, TriggerEvent event, String asyncId) throws InterruptedException, AlreadyExistsException, IOException, KeeperException {
    String parentPath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + event.getSource() + "/" + getName();
    try {
      stateManager.makePath(parentPath);
    } catch (AlreadyExistsException e) {
      // ignore
    }
    return stateManager.createData(parentPath + "/" + PREFIX, Utils.toJSON(Collections.singletonMap("requestid", asyncId)), CreateMode.PERSISTENT_SEQUENTIAL);
  }

}
