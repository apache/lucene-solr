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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.synchronizeddisruption.SynchronizedDisruptionManager;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.SYNCHRONIZED_DISRUPTION_PATH;

/**
 * A request handler to handle the listing, adding, and removing of synchronized disruptions
 */
public class SynchronizedDisruptionHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final CoreContainer coreContainer;
  private final SynchronizedDisruptionManager disruptionManager;

  public static final String ACTION_PARAM = "action";
  public static final String ADD_PARAM = "add";
  public static final String ADD_CLASS_PARAM = "class";
  public static final String ADD_REMOVE_NAME_PARAM = "name";
  public static final String REMOVE_PARAM = "remove";
  public static final String LIST_PARAM = "list";
  public static final String DISTRIBUTE_PARAM = "distribute";
  public static final String ADD_CRON_PARAM = "cron";

  public SynchronizedDisruptionHandler() {
    this.disruptionManager = null;
    coreContainer = null;
  }

  public SynchronizedDisruptionHandler(SynchronizedDisruptionManager disruptionManager, CoreContainer coreContainer) {
    this.disruptionManager = disruptionManager;
    this.coreContainer = coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (coreContainer == null)
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "CoreContainer instance not initialized");

    if (!coreContainer.isZooKeeperAware())
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "Solr must be in Cloud Mode to use this handler");

    if (disruptionManager == null)
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "SynchronizedDisruptionManager instance not initialized");

    handleRequest(req.getParams(), rsp::add);
  }

  public void handleRequest(SolrParams params, BiConsumer<String, Object> consumer) {
    String action = params.get(ACTION_PARAM);
    if (action == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "action is a required param");
    } else {
      if (action.equalsIgnoreCase(ADD_PARAM)) {
        handleAddRequest(params, consumer);
      } else if (action.equalsIgnoreCase(REMOVE_PARAM)) {
        handleRemoveRequest(params, consumer);
      } else if (action.equalsIgnoreCase(LIST_PARAM)) {
        handleListRequest(consumer);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            String.format(Locale.ROOT,"action %s is not a defined action. use %s", action, String.join(",", ADD_PARAM, REMOVE_PARAM, LIST_PARAM)));
      }
    }
  }

  private void handleListRequest(BiConsumer<String, Object> consumer) {
    if (this.disruptionManager != null) {
      consumer.accept("disruptions", this.disruptionManager.listDisruptions());
    } else {
      consumer.accept("errors", "SynchronizedDisruptionManager is not instantiated");
    }
  }

  private void handleRemoveRequest(SolrParams params, BiConsumer<String, Object> consumer) {
    SimpleOrderedMap result = new SimpleOrderedMap();
    SimpleOrderedMap errors = new SimpleOrderedMap();

    String disruptionName = params.get(ADD_REMOVE_NAME_PARAM);
    if (disruptionName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT,"%s action requires %s parameter", REMOVE_PARAM, ADD_REMOVE_NAME_PARAM));
    }

    if (!doesDisruptionExist(disruptionName))
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, String.format(Locale.ROOT,"%s does not exist", disruptionName));

    sendRequestToAllLiveInstances(params, result, errors, "removedDisruptions");

    if (this.disruptionManager != null) {
      boolean isCloseDisruption = this.disruptionManager.closeSynchronizedDisruption(disruptionName, true);
      String closeResult = String.format(Locale.ROOT,"%s has be closed with success value of %s", disruptionName, isCloseDisruption);
      log.debug(closeResult);
      ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
      String baseURL = zkStateReader.getBaseUrlForNodeName(coreContainer.getZkController().getNodeName());
      result.add(baseURL, closeResult);
    } else {
      errors.add("SynchronizedDisruptionManager", "SynchronizedDisruptionManager not instantiated");
    }

    consumer.accept("removedDisruptions", result);
    if (errors.size() > 0) {
      consumer.accept("errors", errors);
    }
  }

  private void handleAddRequest(SolrParams params, BiConsumer<String,Object> consumer) {
    SimpleOrderedMap result = new SimpleOrderedMap();
    SimpleOrderedMap errors = new SimpleOrderedMap();

    String disruptionName = params.get(ADD_REMOVE_NAME_PARAM);
    if (disruptionName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT,"%s action requires %s parameter", ADD_PARAM, ADD_REMOVE_NAME_PARAM));
    }

    if (doesDisruptionExist(disruptionName))
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, String.format(Locale.ROOT,"%s already exists", disruptionName));

    String disruptionClass = params.get(ADD_CLASS_PARAM);
    if (disruptionClass == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT,"%s action requires %s parameter", ADD_PARAM, ADD_CLASS_PARAM));
    }
    String disruptionCron = params.get(ADD_CRON_PARAM);
    if (disruptionCron == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT,"%s action requires %s parameter", ADD_PARAM, ADD_CRON_PARAM));
    }

    //If distribute Param is true (or null) then use this handler to add to zookeeper
    boolean addToZookeeper = params.getBool(DISTRIBUTE_PARAM,true);
    sendRequestToAllLiveInstances(params, result, errors, "addedDisruptions");

    if (this.disruptionManager != null) {
      boolean added = this.disruptionManager.addSynchronizedDisruption(this.coreContainer.getResourceLoader(), disruptionName, disruptionClass, disruptionCron, addToZookeeper);
      String closeResult = String.format(Locale.ROOT,"%s has be added with success value of %s", disruptionName, added);
      log.debug(closeResult);
      ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
      String baseURL = zkStateReader.getBaseUrlForNodeName(coreContainer.getZkController().getNodeName());
      if (!added) {
        errors.add(baseURL, "Failed to be instantiated");
      } else {
        result.add(baseURL, closeResult);
      }
    } else {
      errors.add("SynchronizedDisruptionManager", "SynchronizedDisruptionManager not instantiated");
    }

    consumer.accept("addedDisruptions", result);
    if (errors.size() > 0) {
      consumer.accept("errors", errors);
    }
  }

  private boolean doesDisruptionExist(String disruptionName) {
    return String.join(";", this.disruptionManager.listDisruptions()).contains(disruptionName);
  }

  private void sendRequestToAllLiveInstances(SolrParams params, SimpleOrderedMap result, SimpleOrderedMap errors, String responseKey) {
    boolean isDistribute = params.getBool(DISTRIBUTE_PARAM,true);
    if (isDistribute) {
      CoreContainer coreContainer = this.coreContainer;
      if (coreContainer != null && coreContainer.isZooKeeperAware()) {
        ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
        Set<String> liveNodes = new HashSet<>(coreContainer.getZkController().getSolrCloudManager().getClusterStateProvider().getLiveNodes());
        //Remove this node from live nodes, as it will add the disruption after the others
        String thisNodeName = coreContainer.getZkController().getNodeName();
        liveNodes.remove(thisNodeName);

        ModifiableSolrParams distributedParams = new ModifiableSolrParams(params);
        distributedParams.set(DISTRIBUTE_PARAM, false);
        SolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.POST, SYNCHRONIZED_DISRUPTION_PATH, distributedParams);

        liveNodes.parallelStream().forEach(liveNode -> {
          String baseURL = zkStateReader.getBaseUrlForNodeName(liveNode);
          try (HttpSolrClient solrClient = new HttpSolrClient.Builder()
              .withBaseSolrUrl(baseURL)
              .build()) {
            SolrResponse response = request.process(solrClient);
            log.debug(response.toString());
            if (StringUtils.isEmpty(responseKey))
              result.add(baseURL, response.getResponse().toString());
            else
              result.add(baseURL, ((NamedList)response.getResponse().get(responseKey)).getVal(0).toString());
          } catch (IOException e) {
            log.error("IOException", e);
            errors.add("IOException for " + baseURL, e);
          } catch (SolrServerException e) {
            log.error("SolrServerException", e);
            errors.add("SolrServerException for " + baseURL, e);
          }
        });
      }
    }
  }

  @Override
  public String getDescription() {
    return "Manage Solr Synchronized Disruptions";
  }
}
