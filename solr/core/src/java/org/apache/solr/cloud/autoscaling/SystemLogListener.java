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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.IdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This listener saves events to the {@link CollectionAdminParams#SYSTEM_COLL} collection.
 * <p>Configuration properties:</p>
 * <ul>
 *   <li>collection - optional string, specifies what collection should be used for storing events. Default value
 *   is {@link CollectionAdminParams#SYSTEM_COLL}.</li>
 * </ul>
 */
public class SystemLogListener extends TriggerListenerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOURCE_FIELD = "source_s";
  public static final String SOURCE = SystemLogListener.class.getSimpleName();
  public static final String DOC_TYPE = "autoscaling_event";

  private String collection = CollectionAdminParams.SYSTEM_COLL;
  private boolean enabled = true;

  @Override
  public void init(CoreContainer coreContainer, AutoScalingConfig.TriggerListenerConfig config) {
    super.init(coreContainer, config);
    collection = (String)config.properties.getOrDefault(CollectionAdminParams.COLLECTION, CollectionAdminParams.SYSTEM_COLL);
    enabled = Boolean.parseBoolean(String.valueOf(config.properties.getOrDefault("enabled", true)));
  }

  @Override
  public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context,
               Throwable error, String message) throws Exception {
    if (!enabled) {
      return;
    }
    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
        .withZkHost(coreContainer.getZkController().getZkServerAddress())
        .withHttpClient(coreContainer.getUpdateShardHandler().getHttpClient())
        .build()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(CommonParams.TYPE, DOC_TYPE);
      doc.addField(SOURCE_FIELD, SOURCE);
      doc.addField("id", IdUtils.timeRandomId());
      doc.addField("event.id_s", event.getId());
      doc.addField("event.type_s", event.getEventType().toString());
      doc.addField("event.source_s", event.getSource());
      doc.addField("event.time_l", event.getEventTime());
      doc.addField("timestamp", new Date());
      addMap("event.property.", doc, event.getProperties());
      doc.addField("stage_s", stage.toString());
      if (actionName != null) {
        doc.addField("action_s", actionName);
      }
      if (message != null) {
        doc.addField("message_t", message);
      }
      addError(doc, error);
      // add JSON versions of event and context
      String eventJson = Utils.toJSONString(event);
      doc.addField("event_str", eventJson);
      if (context != null) {
        // capture specifics of operations after compute_plan action
        addOperations(doc, (List<SolrRequest>)context.getProperties().get("operations"));
        // capture specifics of responses after execute_plan action
        addResponses(doc, (List<NamedList<Object>>)context.getProperties().get("responses"));
        addActions("before", doc, (List<String>)context.getProperties().get(TriggerEventProcessorStage.BEFORE_ACTION.toString()));
        addActions("after", doc, (List<String>)context.getProperties().get(TriggerEventProcessorStage.AFTER_ACTION.toString()));
        String contextJson = Utils.toJSONString(context);
        doc.addField("context_str", contextJson);
      }
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      cloudSolrClient.request(req, collection);
    } catch (Exception e) {
      if ((e instanceof SolrException) && e.getMessage().contains("Collection not found")) {
        // relatively benign
        log.info("Collection " + collection + " does not exist, disabling logging.");
        enabled = false;
      } else {
        log.warn("Exception sending event to collection " + collection, e);
      }
    }
  }

  private void addActions(String prefix, SolrInputDocument doc, List<String> actions) {
    if (actions == null) {
      return;
    }
    actions.forEach(a -> doc.addField(prefix + ".actions_ss", a));
  }

  private void addMap(String prefix, SolrInputDocument doc, Map<String, Object> map) {
    map.forEach((k, v) -> {
      if (v instanceof Collection) {
        for (Object o : (Collection)v) {
          doc.addField(prefix + k + "_ss", String.valueOf(o));
        }
      } else {
        doc.addField(prefix + k + "_s", String.valueOf(v));
      }
    });
  }

  private void addOperations(SolrInputDocument doc, List<SolrRequest> operations) {
    if (operations == null || operations.isEmpty()) {
      return;
    }
    for (SolrRequest req : operations) {
      SolrParams params = req.getParams();
      if (params == null) {
        continue;
      }
      // build a whitespace-separated param string
      StringJoiner paramJoiner = new StringJoiner(" ");
      paramJoiner.setEmptyValue("");
      for (Iterator<String> it = params.getParameterNamesIterator(); it.hasNext(); ) {
        final String name = it.next();
        final String [] values = params.getParams(name);
        for (String value : values) {
          paramJoiner.add(name + "=" + value);
        }
      }
      String paramString = paramJoiner.toString();
      if (!paramString.isEmpty()) {
        doc.addField("operations.params_ts", paramString);
      }
    }
  }

  private void addResponses(SolrInputDocument doc, List<NamedList<Object>> responses) {
    if (responses == null || responses.isEmpty()) {
      return;
    }
    for (NamedList<Object> rsp : responses) {
      Object o = rsp.get("success");
      if (o != null) {
        doc.addField("responses_ts", "success " + o);
      } else {
        o = rsp.get("failure");
        if (o != null) {
          doc.addField("responses_ts", "failure " + o);
        } else { // something else
          doc.addField("responses_ts", Utils.toJSONString(rsp));
        }
      }
    }
  }

  private void addError(SolrInputDocument doc, Throwable error) {
    if (error == null) {
      return;
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    error.printStackTrace(pw);
    pw.flush(); pw.close();
    doc.addField("error.message_t", error.getMessage());
    doc.addField("error.details_t", sw.toString());
  }
}
