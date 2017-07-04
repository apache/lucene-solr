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
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for executing cluster operations read from the {@link ActionContext}'s properties
 * with the key name "operations"
 */
public class ExecutePlanAction implements TriggerAction {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Map<String, String> initArgs;

  @Override
  public void close() throws IOException {

  }

  @Override
  public void init(Map<String, String> args) {
    this.initArgs = args;
  }

  @Override
  public String getName() {
    return initArgs.get("name");
  }

  @Override
  public void process(TriggerEvent event, ActionContext context) {
    log.debug("-- processing event: {} with context properties: {}", event, context.getProperties());
    CoreContainer container = context.getCoreContainer();
    List<SolrRequest> operations = (List<SolrRequest>) context.getProperty("operations");
    if (operations == null || operations.isEmpty()) {
      log.info("No operations to execute for event: {}", event);
      return;
    }
    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
        .withZkHost(container.getZkController().getZkServerAddress())
        .withHttpClient(container.getUpdateShardHandler().getHttpClient())
        .build()) {
      for (SolrRequest operation : operations) {
        log.info("Executing operation: {}", operation.getParams());
        try {
          SolrResponse response = operation.process(cloudSolrClient);
          context.getProperties().compute("responses", (s, o) -> {
            List<NamedList<Object>> responses = (List<NamedList<Object>>) o;
            if (responses == null)  responses = new ArrayList<>(operations.size());
            responses.add(response.getResponse());
            return responses;
          });
        } catch (SolrServerException | HttpSolrClient.RemoteSolrException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unexpected exception executing operation: " + operation.getParams(), e);
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected IOException while processing event: " + event, e);
    }
  }
}
